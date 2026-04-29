use half::f16;
use serde::{
    Serialize,
    ser::{
        self, Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
        SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
    },
};
use std::fmt;

/// 估算任意 `Serialize` 值经 CBOR 序列化后的字节大小（不实际写入字节）。
///
/// 如果自定义 `Serialize` 实现返回错误，本函数会 panic；需要显式处理错误时请使用
/// [`try_estimate_cbor_size`]。
pub fn estimate_cbor_size<T: ?Sized + Serialize>(value: &T) -> usize {
    try_estimate_cbor_size(value).expect("CBOR size estimation failed")
}

/// 尝试估算任意 `Serialize` 值经 CBOR 序列化后的字节大小（不实际写入字节）。
pub fn try_estimate_cbor_size<T: ?Sized + Serialize>(value: &T) -> Result<usize, CborSizeError> {
    let mut s = CborSizer { count: 0 };
    value.serialize(&mut s)?;
    Ok(s.count)
}

// ---- CBOR sizer 实现：仅依据 CBOR 头部规则与结构遍历累加大小 ----
/// CBOR 大小估算失败时返回的错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CborSizeError {
    message: String,
}

impl CborSizeError {
    fn new(message: impl fmt::Display) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for CborSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for CborSizeError {}
impl ser::Error for CborSizeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        CborSizeError::new(msg)
    }
}

struct CborSizer {
    count: usize,
}

impl CborSizer {
    #[inline]
    fn add_count(&mut self, len: usize) -> Result<(), CborSizeError> {
        self.count = self
            .count
            .checked_add(len)
            .ok_or_else(|| CborSizeError::new("CBOR size exceeds usize::MAX"))?;
        Ok(())
    }

    #[inline]
    fn add_head_len(&mut self, len: u64) -> Result<(), CborSizeError> {
        // CBOR 头部：1字节(主类型+附加信息) + 可能的长度扩展
        // <24: 1; <= u8: 2; <= u16: 3; <= u32: 5; 否则: 9
        self.add_count(match len {
            0..=23 => 1,
            24..=0xFF => 2,
            0x100..=0xFFFF => 3,
            0x1_0000..=0xFFFF_FFFF => 5,
            _ => 9,
        })
    }

    #[inline]
    fn add_uint(&mut self, v: u64) -> Result<(), CborSizeError> {
        self.add_head_len(v)
    }

    #[inline]
    fn add_nint_i64(&mut self, v: i64) -> Result<(), CborSizeError> {
        // 负整数编码：-1 - n 作为无符号整数长度
        let u = -1i128 - v as i128;
        let u = if u < 0 { 0 } else { u as u64 };
        self.add_head_len(u)
    }

    #[inline]
    fn add_tag(&mut self, tag: u64) -> Result<(), CborSizeError> {
        self.add_head_len(tag)
    }

    #[inline]
    fn add_bytes(&mut self, len: usize) -> Result<(), CborSizeError> {
        self.add_head_len(len as u64)?;
        self.add_count(len)
    }

    #[inline]
    fn add_text(&mut self, len: usize) -> Result<(), CborSizeError> {
        self.add_head_len(len as u64)?;
        self.add_count(len)
    }

    #[inline]
    fn add_array_header(&mut self, len: Option<usize>) -> Result<bool, CborSizeError> {
        match len {
            Some(n) => {
                self.add_head_len(n as u64)?;
                Ok(false)
            }
            None => {
                // 不定长数组起始 0x9f
                self.add_count(1)?;
                Ok(true)
            }
        }
    }

    #[inline]
    fn end_indefinite(&mut self, indefinite: bool) -> Result<(), CborSizeError> {
        if indefinite {
            // break 0xff
            self.add_count(1)?;
        }
        Ok(())
    }

    #[inline]
    fn add_map_header(&mut self, len: Option<usize>) -> Result<bool, CborSizeError> {
        match len {
            Some(n) => {
                self.add_head_len(n as u64)?;
                Ok(false)
            }
            None => {
                // 不定长 map 起始 0xbf
                self.add_count(1)?;
                Ok(true)
            }
        }
    }

    #[inline]
    fn add_f16(&mut self) -> Result<(), CborSizeError> {
        self.add_count(1 /* 头 */ + 2)
    }

    #[inline]
    fn add_f32(&mut self) -> Result<(), CborSizeError> {
        self.add_count(1 /* 头 */ + 4)
    }

    #[inline]
    fn add_f64(&mut self) -> Result<(), CborSizeError> {
        self.add_count(1 /* 头 */ + 8)
    }

    #[inline]
    fn add_simple1(&mut self) -> Result<(), CborSizeError> {
        // 单字节简单值（false/true/null/undefined）：各占 1 字节
        self.add_count(1)
    }

    #[inline]
    fn add_u128(&mut self, v: u128) -> Result<(), CborSizeError> {
        if v <= u64::MAX as u128 {
            return self.add_uint(v as u64);
        }
        // 超过 u64 范围，使用 bignum(tag: 2) + bytes
        self.add_tag(2)?;
        let nbytes = (128 - v.leading_zeros()).div_ceil(8) as usize;
        self.add_bytes(nbytes)
    }

    #[inline]
    fn add_i128(&mut self, v: i128) -> Result<(), CborSizeError> {
        if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
            if v >= 0 {
                self.add_uint(v as u64)?;
            } else {
                self.add_nint_i64(v as i64)?;
            }
            return Ok(());
        }
        // 负大整数使用 tag 3；按 CBOR 规则编码 abs(-1 - v) 的字节串
        if v >= 0 {
            self.add_u128(v as u128)
        } else {
            self.add_tag(3)?;
            let mag = (-1i128 - v) as u128;
            let nbytes = (128 - mag.leading_zeros()).div_ceil(8) as usize;
            self.add_bytes(nbytes)
        }
    }
}

struct StrLenCounter {
    count: usize,
}

impl fmt::Write for StrLenCounter {
    #[inline]
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.count = self.count.checked_add(s.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

struct TagSerializer;

#[inline]
fn unsupported_tag<T>() -> Result<T, CborSizeError> {
    Err(CborSizeError::new("expected unsigned integer CBOR tag"))
}

impl ser::Serializer for TagSerializer {
    type Ok = u64;
    type Error = CborSizeError;

    type SerializeSeq = Impossible<u64, CborSizeError>;
    type SerializeTuple = Impossible<u64, CborSizeError>;
    type SerializeTupleStruct = Impossible<u64, CborSizeError>;
    type SerializeTupleVariant = Impossible<u64, CborSizeError>;
    type SerializeMap = Impossible<u64, CborSizeError>;
    type SerializeStruct = Impossible<u64, CborSizeError>;
    type SerializeStructVariant = Impossible<u64, CborSizeError>;

    #[inline]
    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_i128(self, _v: i128) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }
    #[inline]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }
    #[inline]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }
    #[inline]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v)
    }
    #[inline]
    fn serialize_u128(self, _v: u128) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn collect_str<T: ?Sized + fmt::Display>(self, _value: &T) -> Result<Self::Ok, Self::Error> {
        unsupported_tag()
    }
    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a> ser::Serializer for &'a mut CborSizer {
    type Ok = ();
    type Error = CborSizeError;

    type SerializeSeq = SeqSizer<'a>;
    type SerializeTuple = SeqSizer<'a>;
    type SerializeTupleStruct = SeqSizer<'a>;
    type SerializeTupleVariant = TupleVariantSizer<'a>;
    type SerializeMap = MapSizer<'a>;
    type SerializeStruct = StructSizer<'a>;
    type SerializeStructVariant = StructVariantSizer<'a>;

    #[inline]
    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        self.add_simple1()?;
        Ok(())
    }
    #[inline]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        if v >= 0 {
            self.add_uint(v as u64)?;
        } else {
            self.add_nint_i64(v as i64)?;
        }
        Ok(())
    }
    #[inline]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        if v >= 0 {
            self.add_uint(v as u64)?;
        } else {
            self.add_nint_i64(v as i64)?;
        }
        Ok(())
    }
    #[inline]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        if v >= 0 {
            self.add_uint(v as u64)?;
        } else {
            self.add_nint_i64(v as i64)?;
        }
        Ok(())
    }
    #[inline]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        if v >= 0 {
            self.add_uint(v as u64)?;
        } else {
            self.add_nint_i64(v)?;
        }
        Ok(())
    }
    #[inline]
    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        self.add_i128(v)?;
        Ok(())
    }

    #[inline]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.add_uint(v as u64)?;
        Ok(())
    }
    #[inline]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.add_uint(v as u64)?;
        Ok(())
    }
    #[inline]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.add_uint(v as u64)?;
        Ok(())
    }
    #[inline]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.add_uint(v)?;
        Ok(())
    }
    #[inline]
    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        self.add_u128(v)?;
        Ok(())
    }

    #[inline]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v.into())
    }

    #[inline]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let n16 = f16::from_f64(v);
        let n32 = v as f32;
        let vbits = v.to_bits();
        if f64::from(n16).to_bits() == vbits {
            self.add_f16()?;
        } else if f64::from(n32).to_bits() == vbits {
            self.add_f32()?;
        } else {
            self.add_f64()?;
        };
        Ok(())
    }

    #[inline]
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(&v.to_string())
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.add_text(v.len())?;
        Ok(())
    }

    #[inline]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.add_bytes(v.len())?;
        Ok(())
    }

    #[inline]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        // serde_cbor/ciborium 通常将 None 编码为 null
        self.add_simple1()?;
        Ok(())
    }
    #[inline]
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        // Some(x) 直接编码为 x
        value.serialize(self)
    }

    #[inline]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        // unit -> null
        self.add_simple1()?;
        Ok(())
    }

    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    #[inline]
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        // 透明包装
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        if name == "@@TAG@@" && variant == "@@UNTAGGED@@" {
            return value.serialize(self);
        }

        // { "Variant": value }
        self.add_map_header(Some(1))?;
        self.add_text(variant.len())?;
        value.serialize(self)
    }

    #[inline]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let indefinite = self.add_array_header(len)?;
        Ok(SeqSizer {
            s: self,
            indefinite,
        })
    }

    #[inline]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let indefinite = self.add_array_header(Some(len))?;
        Ok(SeqSizer {
            s: self,
            indefinite,
        })
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        let indefinite = self.add_array_header(Some(len))?;
        Ok(SeqSizer {
            s: self,
            indefinite,
        })
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        if name == "@@TAG@@" && variant == "@@TAGGED@@" {
            return Ok(TupleVariantSizer {
                s: self,
                indefinite: false,
                tag: true,
            });
        }

        // { "Variant": [ ... ] }
        self.add_map_header(Some(1))?;
        self.add_text(variant.len())?;
        let indefinite = self.add_array_header(Some(len))?;
        Ok(TupleVariantSizer {
            s: self,
            indefinite,
            tag: false,
        })
    }

    #[inline]
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let indefinite = self.add_map_header(len)?;
        Ok(MapSizer {
            s: self,
            indefinite,
        })
    }

    #[inline]
    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let indefinite = self.add_map_header(Some(len))?;
        Ok(StructSizer {
            s: self,
            indefinite,
        })
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        // { "Variant": { k:v, ... } }
        self.add_map_header(Some(1))?;
        self.add_text(variant.len())?;
        let indefinite = self.add_map_header(Some(len))?;
        Ok(StructVariantSizer {
            s: self,
            indefinite,
        })
    }

    #[inline]
    fn collect_str<T: ?Sized + fmt::Display>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        let mut counter = StrLenCounter { count: 0 };
        fmt::write(&mut counter, format_args!("{value}"))
            .map_err(|_| CborSizeError::new("failed to count formatted string length"))?;
        self.add_text(counter.count)
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

struct SeqSizer<'a> {
    s: &'a mut CborSizer,
    indefinite: bool,
}
impl SerializeSeq for SeqSizer<'_> {
    type Ok = ();
    type Error = CborSizeError;
    #[inline]
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.s)
    }
    #[inline]
    fn end(self) -> Result<(), Self::Error> {
        self.s.end_indefinite(self.indefinite)
    }
}
impl SerializeTuple for SeqSizer<'_> {
    type Ok = ();
    type Error = CborSizeError;
    #[inline]
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        <Self as SerializeSeq>::serialize_element(self, value)
    }
    #[inline]
    fn end(self) -> Result<(), Self::Error> {
        <Self as SerializeSeq>::end(self)
    }
}
impl SerializeTupleStruct for SeqSizer<'_> {
    type Ok = ();
    type Error = CborSizeError;
    #[inline]
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        <Self as SerializeSeq>::serialize_element(self, value)
    }
    #[inline]
    fn end(self) -> Result<(), Self::Error> {
        <Self as SerializeSeq>::end(self)
    }
}

struct TupleVariantSizer<'a> {
    s: &'a mut CborSizer,
    indefinite: bool,
    tag: bool,
}
impl SerializeTupleVariant for TupleVariantSizer<'_> {
    type Ok = ();
    type Error = CborSizeError;
    #[inline]
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        if self.tag {
            self.tag = false;
            let tag = value.serialize(TagSerializer)?;
            return self.s.add_tag(tag);
        }

        value.serialize(&mut *self.s)
    }
    #[inline]
    fn end(self) -> Result<(), Self::Error> {
        self.s.end_indefinite(self.indefinite)
    }
}

struct MapSizer<'a> {
    s: &'a mut CborSizer,
    indefinite: bool,
}
impl SerializeMap for MapSizer<'_> {
    type Ok = ();
    type Error = CborSizeError;
    #[inline]
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        key.serialize(&mut *self.s)
    }
    #[inline]
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.s)
    }
    #[inline]
    fn end(self) -> Result<(), Self::Error> {
        self.s.end_indefinite(self.indefinite)
    }
}

struct StructSizer<'a> {
    s: &'a mut CborSizer,
    indefinite: bool,
}
impl SerializeStruct for StructSizer<'_> {
    type Ok = ();
    type Error = CborSizeError;
    #[inline]
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.s.add_text(key.len())?;
        value.serialize(&mut *self.s)
    }
    #[inline]
    fn end(self) -> Result<(), Self::Error> {
        self.s.end_indefinite(self.indefinite)
    }
}

struct StructVariantSizer<'a> {
    s: &'a mut CborSizer,
    indefinite: bool,
}
impl SerializeStructVariant for StructVariantSizer<'_> {
    type Ok = ();
    type Error = CborSizeError;
    #[inline]
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.s.add_text(key.len())?;
        value.serialize(&mut *self.s)
    }
    #[inline]
    fn end(self) -> Result<(), Self::Error> {
        self.s.end_indefinite(self.indefinite)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ciborium::into_writer;
    use ciborium::tag::{Accepted, Captured, Required};
    use serde::Serialize;
    use std::collections::BTreeMap;

    fn measured_size<T: ?Sized + Serialize>(v: &T) -> usize {
        let mut buf = Vec::new();
        into_writer(v, &mut buf).expect("serialize with ciborium");
        buf.len()
    }

    fn assert_estimate_eq<T: ?Sized + Serialize>(label: &str, v: &T) {
        let est = estimate_cbor_size(v);
        let real = measured_size(v);
        assert_eq!(
            est, real,
            "CBOR size mismatch for {label}: est={est}, real={real}"
        );
    }

    #[derive(Debug, Serialize)]
    struct S {
        a: u8,
        b: String,
    }

    #[derive(Debug, Serialize)]
    struct N(u64);

    #[derive(Debug, Serialize)]
    enum E {
        A,
        B(u32),
        C { x: u8 },
    }

    #[derive(Debug, Serialize)]
    enum NE {
        V(u64),
    }

    struct BinaryAware;

    impl Serialize for BinaryAware {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            if serializer.is_human_readable() {
                serializer.serialize_str("human readable")
            } else {
                serializer.serialize_bytes(&[1, 2, 3, 4])
            }
        }
    }

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("intentional failure"))
        }
    }

    #[test]
    fn test_cbor_size_primitives() {
        // bool
        assert_estimate_eq("bool:true", &true);
        assert_estimate_eq("bool:false", &false);

        // u64 边界
        for &v in &[
            0u64,
            23,
            24,
            255,
            256,
            65_535,
            65_536,
            u32::MAX as u64,
            u64::MAX,
        ] {
            assert_estimate_eq(&format!("u64:{v}"), &v);
        }

        // i64 边界（包括负数附加信息边界）
        for &v in &[
            -1i64,
            -24,
            -25,
            -255,
            -256,
            -257,
            i32::MIN as i64,
            i32::MAX as i64,
            i64::MIN,
            i64::MAX,
        ] {
            assert_estimate_eq(&format!("i64:{v}"), &v);
        }

        // f32/f64
        assert_estimate_eq("f32:1.0", &1.0f32);
        assert_estimate_eq("f64:1.0", &1.0f64);

        // char（ASCII、3字节、4字节）
        assert_estimate_eq("char:a", &'a');
        assert_estimate_eq("char:中", &'中');
        assert_estimate_eq("char:🦀", &'🦀');
    }

    #[test]
    fn test_cbor_size_text_and_bytes() {
        // 字符串长度边界：0, 23, 24, 255, 256
        let lens = [0usize, 1, 23, 24, 255, 256, 1024];
        for &len in &lens {
            let s = "a".repeat(len);
            assert_estimate_eq(&format!("str:len={len}"), &s);
        }

        // bytes 长度边界：0, 23, 24, 255, 256
        for &len in &lens {
            let v = vec![0xABu8; len];
            assert_estimate_eq(&format!("bytes:len={len}"), &v.as_slice());
        }
    }

    #[test]
    fn test_cbor_size_collections() {
        // Option
        let none_val: Option<u64> = None;
        let some_val: Option<u64> = Some(42);
        assert_estimate_eq("option:none", &none_val);
        assert_estimate_eq("option:some", &some_val);

        // Vec/Seq
        let v: Vec<u64> = (0..30).collect();
        assert_estimate_eq("vec<u64>:30", &v);

        // Tuple
        let t = (1u8, "hi".to_string(), 3u64);
        assert_estimate_eq("tuple(u8,String,u64)", &t);

        // Map（使用 BTreeMap 以固定顺序）
        let mut m: BTreeMap<String, u64> = BTreeMap::new();
        m.insert("a".into(), 1);
        m.insert("b".into(), 2);
        m.insert("long_key".into(), 3);
        assert_estimate_eq("btreemap<string,u64>", &m);
    }

    #[test]
    fn test_cbor_size_structs_enums() {
        // 结构体
        let s = S {
            a: 7,
            b: "hello".into(),
        };
        assert_estimate_eq("struct S", &s);

        // newtype struct
        let n = N(123456789);
        assert_estimate_eq("newtype struct N(u64)", &n);

        // 枚举：unit variant / tuple variant / struct variant
        let e1 = E::A;
        let e2 = E::B(123);
        let e3 = E::C { x: 9 };
        assert_estimate_eq("enum E::A", &e1);
        assert_estimate_eq("enum E::B(123)", &e2);
        assert_estimate_eq("enum E::C{x}", &e3);

        // newtype variant
        let ne = NE::V(888);
        assert_estimate_eq("enum NE::V(u64)", &ne);
    }

    #[test]
    fn test_cbor_size_bignum() {
        // 大整数（超出 u64）
        let big_u: u128 = (u64::MAX as u128) + 1;
        let bigger_u: u128 = 1u128 << 127;
        assert_estimate_eq("u128:u64::MAX+1", &big_u);
        assert_estimate_eq("u128:1<<127", &bigger_u);

        // 大负整数（i128 使用 tag 3）
        let big_neg: i128 = -(1i128 << 100);
        let near_min: i128 = i128::MIN + 1; // 仍远小于 i64::MIN
        assert_estimate_eq("i128:-1<<100", &big_neg);
        assert_estimate_eq("i128:near_min", &near_min);
    }

    #[test]
    fn test_cbor_size_matches_binary_serializer_mode() {
        assert_estimate_eq("binary-aware serialize", &BinaryAware);
    }

    #[test]
    fn test_cbor_size_matches_ciborium_tags() {
        let required = Required::<_, 42>("tagged");
        let accepted = Accepted::<_, 0x1_0000>(123u64);
        let captured_tagged = Captured(Some(7), vec![1u8, 2, 3]);
        let captured_untagged = Captured(None, "plain");

        assert_estimate_eq("tag::Required", &required);
        assert_estimate_eq("tag::Accepted", &accepted);
        assert_estimate_eq("tag::Captured(Some)", &captured_tagged);
        assert_estimate_eq("tag::Captured(None)", &captured_untagged);
    }

    #[test]
    fn test_try_cbor_size_propagates_serialize_errors() {
        let err = try_estimate_cbor_size(&FailingSerialize).unwrap_err();
        assert_eq!(err.to_string(), "intentional failure");
    }
}
