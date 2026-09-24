//! Planning and decoding disjoint ranges with bounded per-request amplification.
use super::*;
use futures::TryStreamExt;

pub(super) fn aligned_range(range: &Range<u64>, chunk: u64, size: u64) -> Range<u64> {
    if range.is_empty() {
        return range.clone();
    }
    range.start / chunk * chunk
        ..(range.end - 1)
            .checked_div(chunk)
            .and_then(|n| n.checked_add(1))
            .and_then(|n| n.checked_mul(chunk))
            .unwrap_or(u64::MAX)
            .min(size)
}

pub(super) fn decrypt_chunk(
    cipher: &Aes256Gcm,
    meta: &Metadata,
    chunk_size: u64,
    index: u64,
    data: &mut [u8],
    location: &Path,
) -> Result<()> {
    let index_usize = usize::try_from(index)
        .map_err(|_| limit_error("EncryptedStore", "chunk index overflow"))?;
    let tag = meta
        .aes_tags
        .get(index_usize)
        .ok_or_else(|| Error::Generic {
            store: "EncryptedStore",
            source: format!("missing AES256 tag for chunk {index} for path {location}").into(),
        })?;
    let aad = chunk_aad_for_meta(meta, chunk_size, index)?;
    let aad: &[u8] = aad.as_ref().map_or(&[], |v| v.as_slice());
    cipher
        .decrypt_inout_detached(
            &Nonce::from(derive_gcm_nonce(&meta.aes_nonce, index)),
            aad,
            data.into(),
            &Tag::from(**tag),
        )
        .map_err(|err| Error::Generic {
            store: "EncryptedStore",
            source: format!("AES256 decrypt failed for path {location}: {err:?}").into(),
        })
}

/// Decrypts a contiguous run of whole chunks starting at chunk `first`, in
/// place when the backend handed over a uniquely owned buffer.
pub(super) fn decrypt_span(
    cipher: &Aes256Gcm,
    meta: &Metadata,
    chunk_size: u64,
    first: u64,
    data: Bytes,
    location: &Path,
) -> Result<Bytes> {
    let mut plaintext = Vec::from(data);
    for (i, chunk) in plaintext.chunks_mut(chunk_size as usize).enumerate() {
        decrypt_chunk(cipher, meta, chunk_size, first + i as u64, chunk, location)?;
    }
    Ok(Bytes::from(plaintext))
}

/// Returns `range` of `bytes`, copied out when it is less than half of the
/// buffer so a small result does not pin a large allocation.
pub(super) fn retain(bytes: &Bytes, range: Range<usize>) -> Bytes {
    if range.len() >= bytes.len().div_ceil(2) {
        bytes.slice(range)
    } else {
        Bytes::copy_from_slice(&bytes[range])
    }
}

struct Fragment {
    index: usize,
    range: Range<u64>,
}
struct Request {
    span: Range<u64>,
    fragments: Vec<Fragment>,
}

fn plan(ranges: &[Range<u64>], chunk_size: u64, size: u64) -> Vec<Request> {
    // A transport block is an integer number of crypto chunks. Sparse reads
    // never coalesce across holes, and adjacent reads never form an unbounded
    // temporary buffer. A single crypto chunk is the irreducible lower bound.
    let block = (8 * 1024 * 1024 / chunk_size).max(1) * chunk_size;
    let mut pieces = Vec::new();
    for (index, range) in ranges.iter().enumerate() {
        let mut start = range.start;
        while start < range.end {
            let end = (start / block)
                .saturating_add(1)
                .saturating_mul(block)
                .min(range.end);
            let piece = start..end;
            pieces.push((
                aligned_range(&piece, chunk_size, size),
                Fragment {
                    index,
                    range: piece,
                },
            ));
            start = end;
        }
    }
    pieces.sort_unstable_by_key(|(span, _)| (span.start, span.end));
    let mut requests: Vec<Request> = Vec::new();
    for (span, fragment) in pieces {
        if let Some(last) = requests.last_mut()
            && span.start <= last.span.end
            && span.start / block == last.span.start / block
        {
            last.span.end = last.span.end.max(span.end);
            last.fragments.push(fragment);
        } else {
            requests.push(Request {
                span,
                fragments: vec![fragment],
            });
        }
    }
    requests
}

pub(super) async fn read_ranges<T: ObjectStore>(
    store: &T,
    payload_path: &Path,
    location: &Path,
    cipher: &Aes256Gcm,
    meta: &Metadata,
    chunk_size: u64,
    ranges: &[Range<u64>],
) -> Result<Vec<Bytes>> {
    let requests = plan(ranges, chunk_size, meta.size);
    let mut stream = futures::stream::iter(requests.into_iter().map(|request| async move {
        let data = store.get_range(payload_path, request.span.clone()).await?;
        if data.len() as u64 != request.span.end - request.span.start {
            return Err(limit_error(
                "EncryptedStore",
                "truncated encrypted data in range response",
            ));
        }
        let plaintext = decrypt_span(
            cipher,
            meta,
            chunk_size,
            request.span.start / chunk_size,
            data,
            location,
        )?;
        Ok::<_, Error>(
            request
                .fragments
                .into_iter()
                .map(|fragment| {
                    let start = (fragment.range.start - request.span.start) as usize;
                    let end = (fragment.range.end - request.span.start) as usize;
                    (
                        fragment.index,
                        fragment.range.start,
                        retain(&plaintext, start..end),
                    )
                })
                .collect::<Vec<_>>(),
        )
    }))
    .buffer_unordered(8);
    let mut parts = vec![Vec::new(); ranges.len()];
    while let Some(fragments) = stream.try_next().await? {
        for (index, offset, data) in fragments {
            parts[index].push((offset, data));
        }
    }
    parts
        .into_iter()
        .zip(ranges)
        .map(|(mut parts, range)| {
            if parts.len() == 1 {
                return Ok(parts.pop().unwrap().1);
            }
            parts.sort_unstable_by_key(|(offset, _)| *offset);
            let len = usize::try_from(range.end - range.start).map_err(|_| {
                limit_error("EncryptedStore", "requested range exceeds address space")
            })?;
            let mut data = Vec::new();
            data.try_reserve_exact(len).map_err(|err| Error::Generic {
                store: "EncryptedStore",
                source: err.into(),
            })?;
            for (_, bytes) in parts {
                data.extend_from_slice(&bytes);
            }
            Ok(Bytes::from(data))
        })
        .collect()
}
