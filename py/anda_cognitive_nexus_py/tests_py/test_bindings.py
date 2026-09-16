"""Module-level surface of the binding."""

import anda_cognitive_nexus_py as anda


def test_kip_version_is_the_protocol_this_binding_speaks():
    # The value a request envelope's `kip` member must carry (§71). A request
    # declaring anything else is refused rather than reinterpreted.
    assert anda.kip_version() == "2.0"
    assert anda.KIP_VERSION == "2.0"


def test_the_bundled_profile_is_available_to_hosts():
    # A host activating its own Schema Packages includes this one when it still
    # wants `Person`, `Preference` and the rest: a Schema Lock names exactly the
    # packages in force, so leaving it out deactivates it (§20.9).
    import json

    profile = json.loads(anda.COGNITIVE_MEMORY_PROFILE)
    assert profile["manifest"]["package_id"] == "kip://profiles/cognitive-memory"
    assert profile["manifest"]["version"] == "2.1.0"
