# Go Test Catalog

**Total Tests:** 1050

**Numbered Tests:** 1050

**Unnumbered Tests:** 0

**Numbered Tests Missing Descriptions:** 0

**Numbering Mismatches:** 80

All numbered test numbers are unique.

This catalog lists all tests in the Go codebase.

| Test # | Function Name | Description | File |
|--------|---------------|-------------|------|
| test001 | `Test001_cap_urn_creation` | TEST1: Test that cap URN is created with tags parsed correctly and direction specs accessible | urn/cap_urn_test.go:33 |
| test002 | `Test002_direction_specs_default_to_wildcard` | TEST2: Test that missing 'in' or 'out' defaults to media: wildcard | urn/cap_urn_test.go:55 |
| test003 | `Test003_direction_matching` | TEST3: Test that direction specs must match exactly, different in/out types don't match, wildcard matches any | urn/cap_urn_test.go:74 |
| test004 | `Test004_unquoted_values_lowercased` | TEST4: Test that unquoted keys and values are normalized to lowercase | urn/cap_urn_test.go:96 |
| test005 | `Test005_quoted_values_preserve_case` | TEST5: Test that quoted values preserve case while unquoted are lowercased | urn/cap_urn_test.go:120 |
| test006 | `Test006_quoted_value_special_chars` | TEST6: Test that quoted values can contain special characters (semicolons, equals, spaces) | urn/cap_urn_test.go:146 |
| test007 | `Test007_quoted_value_escape_sequences` | TEST7: Test that escape sequences in quoted values (\" and \\) are parsed correctly | urn/cap_urn_test.go:167 |
| test008 | `Test008_mixed_quoted_unquoted` | TEST8: Test that mixed quoted and unquoted values in same URN parse correctly | urn/cap_urn_test.go:188 |
| test009 | `Test009_unterminated_quote_error` | TEST9: Test that unterminated quote produces UnterminatedQuote error | urn/cap_urn_test.go:202 |
| test010 | `Test010_invalid_escape_sequence_error` | TEST10: Test that invalid escape sequences (like \n, \x) produce InvalidEscapeSequence error | urn/cap_urn_test.go:212 |
| test011 | `Test011_serialization_smart_quoting` | TEST11: Test that serialization uses smart quoting (no quotes for simple lowercase, quotes for special chars/uppercase) | urn/cap_urn_test.go:229 |
| test012 | `Test012_round_trip_simple` | TEST12: Test that simple cap URN round-trips (parse -> serialize -> parse equals original) | urn/cap_urn_test.go:260 |
| test013 | `Test013_round_trip_quoted` | TEST13: Test that quoted values round-trip preserving case and spaces | urn/cap_urn_test.go:271 |
| test014 | `Test014_round_trip_escapes` | TEST14: Test that escape sequences round-trip correctly | urn/cap_urn_test.go:285 |
| test015 | `Test015_cap_prefix_required` | TEST15: Test that cap: prefix is required and case-insensitive | urn/cap_urn_test.go:299 |
| test016 | `Test016_trailing_semicolon_equivalence` | TEST16: Test that trailing semicolon is equivalent (same hash, same string, matches) | urn/cap_urn_test.go:316 |
| test017 | `Test017_tag_matching` | TEST17: Test tag matching: exact match, subset match, wildcard match, value mismatch | urn/cap_urn_test.go:352 |
| test018 | `Test018_matching_case_sensitive_values` | TEST18: Test that quoted values with different case do NOT match (case-sensitive) | urn/cap_urn_test.go:382 |
| test019 | `Test019_missing_tag_handling` | TEST19: Missing tag in instance causes rejection — pattern's tags are constraints | urn/cap_urn_test.go:396 |
| test020 | `Test020_specificity` | TEST20: Specificity is the sum of per-tag truth-table scores across in/out/y. Marker tags (bare segments like `generate`) are must-have-any (score 2), exact `key=value` tags score 3, missing/`?` score 0, `!` scores 1. test_urn() builds "cap:in=media:void;out=media:record;<tags>" so the directional baseline is: in:  media:void  -> {void=*}     -> 2 out: media:record -> {record=*}  -> 2 Total directional baseline: 4. | urn/cap_urn_test.go:425 |
| test021 | `Test021_builder` | TEST21: Test builder creates cap URN with correct tags and direction specs | urn/cap_urn_test.go:454 |
| test022 | `Test022_builder_requires_direction` | TEST22: Test builder requires both in_spec and out_spec | urn/cap_urn_test.go:471 |
| test023 | `Test023_builder_preserves_case` | TEST23: Test builder lowercases keys but preserves value case | urn/cap_urn_test.go:492 |
| test024 | `Test024_directional_accepts` | TEST24: Directional accepts — pattern's tags are constraints, instance must satisfy | urn/cap_urn_test.go:506 |
| test025 | `Test025_best_match` | TEST25: Test find_best_match returns most specific matching cap | urn/cap_urn_test.go:536 |
| test026 | `Test026_merge_and_subset` | TEST26: Test merge combines tags from both caps, subset keeps only specified tags | urn/cap_urn_test.go:565 |
| test027 | `Test027_wildcard_tag` | TEST27: Test with_wildcard_tag sets tag to wildcard, including in/out | urn/cap_urn_test.go:597 |
| test28 | `Test28_empty_cap_urn_is_illegal` | TEST28: Test empty cap URN is illegal after effect transition | urn/cap_urn_test.go:1968 |
| test029 | `Test029_minimal_cap_urn` | TEST29: Test minimal valid cap URN has just in and out, empty tags | urn/cap_urn_test.go:631 |
| test030 | `Test030_extended_character_support` | TEST30: Test extended characters (forward slashes, colons) in tag values | urn/cap_urn_test.go:640 |
| test031 | `Test031_wildcard_restrictions` | TEST31: Test wildcard rejected in keys but accepted in values | urn/cap_urn_test.go:655 |
| test032 | `Test032_duplicate_key_rejection` | TEST32: Test duplicate keys are rejected with DuplicateKey error | urn/cap_urn_test.go:673 |
| test033 | `Test033_numeric_key_restriction` | TEST33: Test pure numeric keys rejected, mixed alphanumeric allowed, numeric values allowed | urn/cap_urn_test.go:683 |
| test034 | `Test034_empty_value_error` | TEST34: Test empty values are rejected | urn/cap_urn_test.go:709 |
| test035 | `Test035_has_tag_case_sensitive` | TEST35: Test has_tag is case-sensitive for values, case-insensitive for keys, works for in/out | urn/cap_urn_test.go:720 |
| test036 | `Test036_with_tag_preserves_value` | TEST36: Test with_tag preserves value case | urn/cap_urn_test.go:734 |
| test037 | `Test037_with_tag_rejects_empty_value` | TEST37: Test with_tag rejects empty value | urn/cap_urn_test.go:744 |
| test038 | `Test038_semantic_equivalence` | TEST38: Test semantic equivalence of unquoted and quoted simple lowercase values | urn/cap_urn_test.go:752 |
| test039 | `Test039_get_tag_returns_direction_specs` | TEST39: Test get_tag returns direction specs (in/out) with case-insensitive lookup | urn/cap_urn_test.go:761 |
| test040 | `Test040_matching_semantics_exact_match` | TEST40: Matching semantics - exact match succeeds | urn/cap_urn_test.go:785 |
| test041 | `Test041_matching_semantics_cap_missing_tag` | TEST41: Matching semantics - cap missing tag matches (implicit wildcard) | urn/cap_urn_test.go:796 |
| test042 | `Test042_matching_semantics_cap_has_extra_tag` | TEST42: Pattern rejects instance missing required tags | urn/cap_urn_test.go:813 |
| test043 | `Test043_matching_semantics_request_has_wildcard` | TEST43: Matching semantics - request wildcard matches specific cap value | urn/cap_urn_test.go:830 |
| test044 | `Test044_matching_semantics_cap_has_wildcard` | TEST44: Matching semantics - cap wildcard matches specific request value | urn/cap_urn_test.go:841 |
| test045 | `Test045_matching_semantics_value_mismatch` | TEST45: Matching semantics - value mismatch does not match | urn/cap_urn_test.go:852 |
| test046 | `Test046_matching_semantics_fallback_pattern` | TEST46: Matching semantics - fallback pattern (cap missing tag = implicit wildcard) | urn/cap_urn_test.go:863 |
| test047 | `Test047_matching_semantics_thumbnail_void_input` | TEST47: Matching semantics - thumbnail fallback with void input | urn/cap_urn_test.go:879 |
| test048 | `Test048_matching_semantics_wildcard_direction_matches_anything` | TEST48: Matching semantics - wildcard direction matches anything | urn/cap_urn_test.go:896 |
| test049 | `Test049_matching_semantics_cross_dimension_independence` | TEST49: Non-overlapping tags — neither direction accepts | urn/cap_urn_test.go:911 |
| test050 | `Test050_matching_semantics_direction_mismatch` | TEST50: Matching semantics - direction mismatch prevents matching | urn/cap_urn_test.go:928 |
| test051 | `Test051_input_validation_success` | TEST51: Test input validation succeeds with valid positional argument | cap/validation_test.go:58 |
| test052 | `Test052_input_validation_missing_required` | TEST52: Test input validation fails with MissingRequiredArgument when required arg missing | cap/validation_test.go:83 |
| test053 | `Test053_input_validation_wrong_type` | TEST53: Test input validation fails with InvalidArgumentType when wrong type provided | cap/validation_test.go:101 |
| test060 | `Test060_wrong_prefix_fails` | TEST60: Test wrong prefix fails with InvalidPrefix error showing expected and actual prefix | urn/media_urn_test.go:39 |
| test062 | `Test062_is_record` | TEST62: Test is_record returns true when record marker tag is present indicating key-value structure | urn/media_urn_test.go:56 |
| test063 | `Test063_is_scalar` | TEST63: Test is_scalar returns true when list marker tag is absent (scalar is default) | urn/media_urn_test.go:85 |
| test064 | `Test064_is_list` | TEST64: Test is_list returns true when list marker tag is present indicating ordered collection | urn/media_urn_test.go:106 |
| test065 | `Test065_is_opaque` | TEST65: Test is_opaque returns true when record marker is absent (opaque is default) | urn/media_urn_test.go:121 |
| test066 | `Test066_is_json` | TEST66: Test is_json returns true only when json marker tag is present for JSON representation | urn/media_urn_test.go:152 |
| test067 | `Test067_is_text` | TEST67: Text-representability is now carried by the orthogonal `enc=` tag (the old `textable` marker and is_text() are gone). A media is "text" iff it declares an encoding. enc is orthogonal to format/numeric, so only media that actually carry enc= are text. | urn/media_urn_test.go:167 |
| test068 | `Test068_is_void` | TEST68: Test is_void returns true when void flag or type=void tag is present | urn/media_urn_test.go:192 |
| test071 | `Test071_to_string_roundtrip` | TEST71: Test to_string roundtrip ensures serialization and deserialization preserve URN structure | urn/media_urn_test.go:220 |
| test072 | `Test072_constants_parse` | TEST72: Test all media URN constants parse successfully as valid media URNs | urn/media_urn_test.go:233 |
| test073 | `Test073_extension_helpers` | TEST73: Test extension helper functions create media URNs with ext tag and correct format | urn/media_urn_test.go:273 |
| test074 | `Test074_media_urn_matching` | TEST74: Test media URN conforms_to using tagged URN semantics with specific and generic requirements | urn/media_urn_test.go:282 |
| test075 | `Test075_matching` | TEST75: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests | urn/media_urn_test.go:303 |
| test076 | `Test076_specificity` | TEST76: Test specificity increases with more tags for ranking conformance | urn/media_urn_test.go:315 |
| test077 | `Test077_serde_roundtrip` | TEST77: Test serde roundtrip serializes to JSON string and deserializes back correctly | urn/media_urn_test.go:327 |
| test078 | `Test078_object_does_not_conform_to_string` | TEST78: conforms_to behavior between MEDIA_OBJECT and MEDIA_STRING | urn/media_urn_test.go:344 |
| test088 | `Test088_resolve_seeded_spec` | TEST88: Resolving a media URN seeded into the registry returns the seeded spec verbatim. A regression in the registry-resolution path would surface as a `None`-shaped result here, since there is no local-override fallback to mask it. | media/spec_test.go:42 |
| test089 | `Test089_resolve_seeded_record_spec` | TEST89: A seeded record-shaped media def carries its schema and profile_uri intact through resolution. Catches a regression that dropped optional fields when copying into ResolvedMediaDef. | media/spec_test.go:56 |
| test0090 | `Test0090_absent_scan_root_yields_empty_roster` | TEST90: Absent scan root yields empty roster | bifaci/cartridge_discovery_test.go:65 |
| test0091 | `Test0091_missing_cartridge_json_is_manifest_invalid` | TEST91: Missing cartridge json is manifest invalid | bifaci/cartridge_discovery_test.go:73 |
| test0092 | `Test0092_channel_mismatch_is_bad_installation` | TEST92: Channel mismatch is bad installation | bifaci/cartridge_discovery_test.go:82 |
| test93 | `Test93_resolve_unresolvable_fails_hard` | TEST93: Test resolving unknown media URN fails with UnresolvableMediaUrn error | media/spec_test.go:86 |
| test0094 | `Test0094_fabric_manifest_mismatch_is_flagged` | TEST94: Fabric manifest mismatch is flagged | bifaci/cartridge_discovery_test.go:93 |
| test095 | `Test095_media_def_def_serialize` | TEST95: Test MediaDef serializes with required fields and skips None fields | media/spec_test.go:103 |
| test096 | `Test096_media_def_def_deserialize` | TEST96: Test deserializing MediaDef from JSON object | media/spec_test.go:129 |
| test097 | `Test097_validate_no_duplicate_urns_catches_duplicates` | TEST97: Test duplicate URN validation catches duplicates | media/spec_test.go:145 |
| test098 | `Test098_validate_no_duplicate_urns_passes_for_unique` | TEST98: Test duplicate URN validation passes for unique URNs | media/spec_test.go:157 |
| test99 | `Test99_resolved_is_binary` | TEST99: A media def with no enc= tag is not text-representable. The old is_binary/is_text axis is gone; text is identified by the presence of an encoding (HasEncoding), so "binary" is simply the absence of one. | media/spec_test.go:173 |
| test100 | `Test100_resolved_is_map` | TEST100: Test ResolvedMediaDef is_record returns true when record marker is present | media/spec_test.go:191 |
| test101 | `Test101_resolved_is_scalar` | TEST101: Test ResolvedMediaDef is_scalar returns true when list marker is absent | media/spec_test.go:210 |
| test102 | `Test102_resolved_is_list` | TEST102: Test ResolvedMediaDef is_list returns true when list marker is present | media/spec_test.go:228 |
| test103 | `Test103_resolved_is_json` | TEST103: Test ResolvedMediaDef is_json returns true when json tag is present | media/spec_test.go:246 |
| test104 | `Test104_resolved_is_text` | TEST104: Test ResolvedMediaDef is_text returns true when enc tag is present | media/spec_test.go:263 |
| test105 | `Test105_metadata_propagation` | TEST105: Test metadata propagates from media def def to resolved media def | media/spec_test.go:284 |
| test106 | `Test106_metadata_with_validation` | TEST106: Test metadata and validation can coexist in media definition | media/spec_test.go:312 |
| test107 | `Test107_extensions_propagation` | TEST107: Test extensions field propagates from media def def to resolved | media/spec_test.go:355 |
| test108 | `Test108_cap_creation` | TEST108: Test creating new cap with URN, title, and command verifies correct initialization | cap/definition_test.go:23 |
| test109 | `Test109_cap_with_metadata` | TEST109: Test creating cap with metadata initializes and retrieves metadata correctly | cap/definition_test.go:42 |
| test110 | `Test110_cap_matching` | TEST110: Test cap matching with subset semantics for request fulfillment | cap/definition_test.go:68 |
| test111 | `Test111_cap_title` | TEST111: Test getting and setting cap title updates correctly | cap/definition_test.go:82 |
| test112 | `Test112_cap_definition_equality` | TEST112: Test cap equality based on URN and title matching | cap/definition_test.go:97 |
| test113 | `Test113_cap_stdin` | TEST113: Test cap stdin support via args with stdin source and serialization roundtrip | cap/definition_test.go:113 |
| test114 | `Test114_arg_source_types` | TEST114: Test ArgSource type variants stdin, position, and cli_flag with their accessors | cap/definition_test.go:151 |
| test115 | `Test115_cap_arg_serialization` | TEST115: Test CapArg serialization and deserialization with multiple sources | cap/definition_test.go:181 |
| test116 | `Test116_cap_arg_constructors` | TEST116: Test CapArg constructor methods basic and with_description create args correctly | cap/definition_test.go:223 |
| test0120 | `Test0120_registry_url_under_dev_slug_is_rejected` | TEST120: Registry url under dev slug is rejected | bifaci/cartridge_discovery_test.go:103 |
| test0123 | `Test0123_CapExists` | TEST0123: Cap exists | cap/registry_test.go:113 |
| test0124 | `Test0124_ParseHeadersWithNoWiringsReturnsNoEdgesError` | Test0124_ParseHeadersWithNoWiringsReturnsNoEdgesError verifies the ErrNoEdges case. | machine/machine_test.go:455 |
| test0125 | `Test0125_effect_none_preserves_runtime_media` | TEST0125: effect=none preserves runtime media identity | urn/cap_urn_test.go:2026 |
| test0126 | `Test0126_effect_declared_uses_declared_output` | TEST0126: default effect=declared uses the declared output | urn/cap_urn_test.go:2044 |
| test0127 | `Test0127_invalid_effect_none_fails_hard` | TEST0127: invalid effect=none declarations fail hard | urn/cap_urn_test.go:2056 |
| test0128 | `Test0128_effect_dispatch_requires_explicit_wildcard` | TEST0128: omitted effect means declared; unconstrained effect must be explicit | urn/cap_urn_test.go:2061 |
| test133 | `Test133_ReattachByIDPreservesSlotIndex` | Reattach-by-id tests for the cardinality-stable slot model. When a master dies and the host reconnects, the new socket MUST attach to the same slot index — preserving routing entries keyed by index. Accumulating zombie slots on each reconnect was the bug class these tests guard against. | bifaci/relay_switch_test.go:963 |
| test134 | `Test134_AddMasterWithDuplicateHealthyIDErrors` | TEST134: Add master with duplicate healthy i d errors | bifaci/relay_switch_test.go:1050 |
| test138 | `Test138_parse_registry_json_with_stdin` | TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction | cap/registry_test.go:95 |
| test141 | `Test141_per_cap_url_shape` | TEST141: URL has the right shape — protocol, host, /caps/ prefix, 64 hex chars, no extension. | cap/registry_test.go:156 |
| test142 | `Test142_normalize_handles_different_tag_orders` | TEST142: Different tag orders normalise to the same URL — the canonicaliser strips the variation before hashing. | cap/registry_test.go:168 |
| test143 | `Test143_default_config` | TEST143: Default config points at https://fabric.capdag.com/ unless overridden by CDG_FABRIC_REGISTRY_URL. | cap/registry_test.go:179 |
| test144 | `Test144_custom_registry_url` | TEST144: Test custom registry URL updates both registry and schema base URLs | cap/registry_test.go:192 |
| test145 | `Test145_custom_registry_and_schema_url` | TEST145: Test custom registry and schema URLs set independently | cap/registry_test.go:200 |
| test146 | `Test146_schema_url_not_overwritten_when_explicit` | TEST146: Test schema URL not overwritten when set explicitly before registry URL | cap/registry_test.go:209 |
| test147 | `Test147_registry_for_test_with_config` | TEST147: Test registry for test with custom config creates registry with specified URLs | cap/registry_test.go:219 |
| test148 | `Test148_cap_manifest_creation` | TEST148: Manifest creation with cap groups | bifaci/manifest_test.go:23 |
| test149 | `Test149_cap_manifest_with_author` | TEST149: Author field | bifaci/manifest_test.go:45 |
| test150 | `Test150_cap_manifest_json_serialization` | TEST150: JSON roundtrip | bifaci/manifest_test.go:85 |
| test151 | `Test151_cap_manifest_required_fields` | TEST151: Missing required fields fail | bifaci/manifest_test.go:145 |
| test152 | `Test152_cap_manifest_with_multiple_caps` | TEST152: Multiple caps across groups | bifaci/manifest_test.go:154 |
| test153 | `Test153_cap_manifest_empty_cap_groups` | TEST153: Empty cap groups | bifaci/manifest_test.go:178 |
| test154 | `Test154_cap_manifest_optional_fields` | TEST154: Optional author field omitted in serialization | bifaci/manifest_test.go:197 |
| test155 | `Test155_component_metadata_interface` | TEST155: ComponentMetadata trait | bifaci/manifest_test.go:239 |
| test156 | `Test156_StdinSourceDataCreation` | TEST156: Test creating StdinSource Data variant with byte vector | cap/caller_test.go:12 |
| test157 | `Test157_StdinSourceFileReferenceCreation` | TEST157: Test creating StdinSource FileReference variant with all required fields | cap/caller_test.go:21 |
| test158 | `Test158_StdinSourceEmptyData` | TEST158: Test StdinSource Data with empty vector stores and retrieves correctly | cap/caller_test.go:37 |
| test159 | `Test159_StdinSourceBinaryContent` | TEST159: Test StdinSource Data with binary content like PNG header bytes | cap/caller_test.go:45 |
| test160 | `Test160_StdinSourceClone` | TEST160: Test StdinSource Data clone creates independent copy with same data | cap/caller_test.go:57 |
| test161 | `Test161_StdinSourceFileReferenceClone` | TEST161: Test StdinSource FileReference clone creates independent copy with same fields | cap/caller_test.go:68 |
| test162 | `Test162_StdinSourceDebug` | TEST162: Test StdinSource Debug format displays variant type and relevant fields | cap/caller_test.go:86 |
| test163 | `Test163_schema_validator_validate_argument_with_schema_success` | TEST163: Test argument schema validation succeeds with valid JSON matching schema | cap/schema_validation_test.go:14 |
| test164 | `Test164_schema_validator_validate_argument_with_schema_failure` | TEST164: Test argument schema validation fails with JSON missing required fields | cap/schema_validation_test.go:53 |
| test165 | `Test165_schema_validator_validate_output_with_schema_success` | TEST165: Test output schema validation succeeds with valid JSON matching schema | cap/schema_validation_test.go:116 |
| test166 | `Test166_schema_validator_skip_validation_without_schema` | TEST166: Test validation skipped when resolved media def has no schema | cap/schema_validation_test.go:689 |
| test167 | `Test167_schema_validator_unresolvable_media_urn_fails_hard` | TEST167: Test validation fails hard when media URN cannot be resolved from any source | cap/schema_validation_test.go:714 |
| test168 | `Test168_response_wrapper_from_json` | TEST168: Test ResponseWrapper from JSON deserializes to correct structured type | cap/response_test.go:22 |
| test169 | `Test169_response_wrapper_as_int` | TEST169: Test ResponseWrapper converts to primitive types integer, float, boolean, string | cap/response_test.go:76 |
| test170 | `Test170_response_wrapper_from_binary` | TEST170: Test ResponseWrapper from binary stores and retrieves raw bytes correctly | cap/response_test.go:59 |
| test171 | `Test171_frame_type_roundtrip` | TEST171: Test all FrameType discriminants roundtrip through u8 conversion preserving identity | bifaci/frame_test.go:13 |
| test172 | `Test172_frame_type_valid_range` | TEST172: Test FrameType::from_u8 returns None for values outside the valid discriminant range | bifaci/frame_test.go:38 |
| test173 | `Test173_frame_type_wire_protocol_values` | TEST173: Test FrameType discriminant values match the wire protocol specification exactly | bifaci/frame_test.go:71 |
| test174 | `Test174_message_id_new_uuid_roundtrip` | TEST174: Test MessageId::new_uuid generates valid UUID that roundtrips through string conversion | bifaci/frame_test.go:106 |
| test175 | `Test175_message_id_uuid_distinct` | TEST175: Test two MessageId::new_uuid calls produce distinct IDs (no collisions) | bifaci/frame_test.go:127 |
| test176 | `Test176_message_id_uint_no_uuid_string` | TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns None | bifaci/frame_test.go:137 |
| test177 | `Test177_message_id_from_invalid_uuid_str` | TEST177: Test MessageId::from_uuid_str rejects invalid UUID strings | bifaci/frame_test.go:150 |
| test178 | `Test178_message_id_as_bytes` | TEST178: Test MessageId::as_bytes produces correct byte representations for Uuid and Uint variants | bifaci/frame_test.go:160 |
| test179 | `Test179_message_id_default` | TEST179: Test MessageId::default creates a UUID variant (not Uint) | bifaci/frame_test.go:181 |
| test180 | `Test180_frame_hello_without_manifest` | TEST180: Test Frame::hello without manifest produces correct HELLO frame for host side | bifaci/frame_test.go:192 |
| test181 | `Test181_frame_hello_with_manifest` | TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes for cartridge side | bifaci/frame_test.go:207 |
| test182 | `Test182_frame_req` | TEST182: Test Frame::req stores cap URN, payload, and content_type correctly | bifaci/frame_test.go:223 |
| test0183 | `Test0183_IntegrationMediaUrnResolution` | Test0183_IntegrationMediaUrnResolution verifies media URN resolution | bifaci/integration_test.go:163 |
| test184 | `Test184_frame_chunk` | TEST184: Test Frame::chunk stores seq and payload for streaming (with stream_id) | bifaci/frame_test.go:248 |
| test185 | `Test185_frame_err` | TEST185: Test Frame::err stores error code and message in metadata | bifaci/frame_test.go:273 |
| test186 | `Test186_frame_log` | TEST186: Test Frame::log stores level and message in metadata | bifaci/frame_test.go:292 |
| test187 | `Test187_frame_end_with_payload` | TEST187: Test Frame::end with payload sets eof and optional final payload | bifaci/frame_test.go:311 |
| test188 | `Test188_frame_end_without_payload` | TEST188: Test Frame::end without payload still sets eof marker | bifaci/frame_test.go:329 |
| test189 | `Test189_frame_chunk_with_offset` | TEST189: Test chunk_with_offset sets offset on all chunks but len only on seq=0 (with stream_id) | bifaci/frame_test.go:345 |
| test190 | `Test190_frame_heartbeat` | TEST190: Test Frame::heartbeat creates minimal frame with no payload or metadata | bifaci/frame_test.go:395 |
| test191 | `Test191_error_accessors_on_non_err_frame` | TEST191: Test error_code and error_message return None for non-Err frame types | bifaci/frame_test.go:411 |
| test192 | `Test192_log_accessors_on_non_log_frame` | TEST192: Test log_level and log_message return None for non-Log frame types | bifaci/frame_test.go:427 |
| test193 | `Test193_hello_accessors_on_non_hello_frame` | TEST193: Test hello_max_frame and hello_max_chunk return None for non-Hello frame types | bifaci/frame_test.go:438 |
| test194 | `Test194_frame_new_defaults` | TEST194: Test Frame::new sets version and defaults correctly, optional fields are None | bifaci/frame_test.go:449 |
| test195 | `Test195_frame_default_type` | TEST195: Test Frame::default creates a Req frame (the documented default) | bifaci/frame_test.go:489 |
| test196 | `Test196_is_eof_when_none` | TEST196: Test is_eof returns false when eof field is None (unset) | bifaci/frame_test.go:500 |
| test197 | `Test197_is_eof_when_false` | TEST197: Test is_eof returns false when eof field is explicitly Some(false) | bifaci/frame_test.go:508 |
| test198 | `Test198_limits_default` | TEST198: Test Limits::default provides the documented default values | bifaci/frame_test.go:518 |
| test199 | `Test199_protocol_version_constant` | TEST199: Test PROTOCOL_VERSION is 2 | bifaci/frame_test.go:536 |
| test200 | `Test200_key_constants` | TEST200: Test integer key constants match the protocol specification | bifaci/frame_test.go:543 |
| test201 | `Test201_hello_manifest_binary_data` | TEST201: Test hello_with_manifest preserves binary manifest data (not just JSON text) | bifaci/frame_test.go:580 |
| test202 | `Test202_message_id_equality_and_hash` | TEST202: Test MessageId Eq/Hash semantics: equal UUIDs are equal, different ones are not | bifaci/frame_test.go:602 |
| test203 | `Test203_message_id_cross_variant_inequality` | TEST203: Test Uuid and Uint variants of MessageId are never equal even for coincidental byte values | bifaci/frame_test.go:630 |
| test204 | `Test204_req_frame_empty_payload` | TEST204: Test Frame::req with empty payload stores Some(empty vec) not None | bifaci/frame_test.go:641 |
| test205 | `Test205_req_frame_roundtrip` | TEST205: Test REQ frame encode/decode roundtrip preserves all fields | bifaci/io_test.go:28 |
| test206 | `Test206_hello_frame_roundtrip` | TEST206: Test HELLO frame encode/decode roundtrip preserves max_frame, max_chunk, max_reorder_buffer | bifaci/io_test.go:60 |
| test207 | `Test207_err_frame_roundtrip` | TEST207: Test ERR frame encode/decode roundtrip preserves error code and message | bifaci/io_test.go:85 |
| test208 | `Test208_log_frame_roundtrip` | TEST208: Test LOG frame encode/decode roundtrip preserves level and message | bifaci/io_test.go:110 |
| test0209 | `Test0209_IntegrationMediaDefConstruction` | Test0209_IntegrationMediaDefConstruction verifies media.MediaDef construction | bifaci/integration_test.go:214 |
| test210 | `Test210_end_frame_roundtrip` | TEST210: Test END frame encode/decode roundtrip preserves eof marker and optional payload | bifaci/io_test.go:137 |
| test211 | `Test211_hello_with_manifest_roundtrip` | TEST211: Test HELLO with manifest encode/decode roundtrip preserves manifest bytes and limits | bifaci/io_test.go:164 |
| test212 | `Test212_chunk_with_offset_roundtrip` | TEST212: Test chunk_with_offset encode/decode roundtrip preserves offset, len, eof (with stream_id) | bifaci/io_test.go:190 |
| test213 | `Test213_heartbeat_roundtrip` | TEST213: Test heartbeat frame encode/decode roundtrip preserves ID with no extra fields | bifaci/io_test.go:234 |
| test214 | `Test214_frame_io_roundtrip` | TEST214: Test write_frame/read_frame IO roundtrip through length-prefixed wire format | bifaci/io_test.go:257 |
| test215 | `Test215_read_multiple_frames` | TEST215: Test reading multiple sequential frames from a single buffer | bifaci/io_test.go:282 |
| test216 | `Test216_write_frame_rejects_oversized` | TEST216: Test write_frame rejects frames exceeding max_frame limit | bifaci/io_test.go:316 |
| test217 | `Test217_read_frame_rejects_oversized` | TEST217: Test read_frame rejects incoming frames exceeding the negotiated max_frame limit | bifaci/io_test.go:335 |
| test218 | `Test218_write_chunked` | TEST218: Test write_chunked splits data into chunks respecting max_chunk and reconstructs correctly Chunks from write_chunked have seq=0. SeqAssigner at the output stage assigns final seq. Chunk ordering within a stream is tracked by chunk_index (chunk_index field). | bifaci/io_test.go:356 |
| test219 | `Test219_write_chunked_empty` | TEST219: Test write_chunked with empty data produces a single EOF chunk | bifaci/io_test.go:414 |
| test220 | `Test220_write_chunked_exact_chunk_size` | TEST220: Test write_chunked with data exactly equal to max_chunk produces exactly one chunk | bifaci/io_test.go:457 |
| test221 | `Test221_read_frame_eof` | TEST221: Test read_frame returns Ok(None) on clean EOF (empty stream) | bifaci/io_test.go:494 |
| test222 | `Test222_read_frame_truncated_length_prefix` | TEST222: Test read_frame handles truncated length prefix (fewer than 4 bytes available) | bifaci/io_test.go:505 |
| test223 | `Test223_read_frame_truncated_body` | TEST223: Test read_frame returns error on truncated frame body (length prefix says more bytes than available) | bifaci/io_test.go:516 |
| test224 | `Test224_message_id_uint_roundtrip` | TEST224: Test MessageId::Uint roundtrips through encode/decode | bifaci/io_test.go:532 |
| test225 | `Test225_decode_non_map_value` | TEST225: Test decode_frame rejects non-map CBOR values (e.g., array, integer, string) | bifaci/io_test.go:552 |
| test226 | `Test226_decode_missing_version` | TEST226: Test decode_frame rejects CBOR map missing required version field | bifaci/io_test.go:563 |
| test227 | `Test227_decode_invalid_frame_type_value` | TEST227: Test decode_frame rejects CBOR map with invalid frame_type value | bifaci/io_test.go:578 |
| test228 | `Test228_decode_missing_id` | TEST228: Test decode_frame rejects CBOR map missing required id field | bifaci/io_test.go:592 |
| test229 | `Test229_frame_reader_writer_set_limits` | TEST229: Test FrameReader/FrameWriter set_limits updates the negotiated limits | bifaci/io_test.go:606 |
| test230 | `Test230_sync_handshake` | TEST230: Test async handshake exchanges HELLO frames and negotiates minimum limits | bifaci/io_test.go:630 |
| test231 | `Test231_handshake_rejects_non_hello` | TEST231: Test handshake fails when peer sends non-HELLO frame | bifaci/io_test.go:728 |
| test232 | `Test232_handshake_rejects_missing_manifest` | TEST232: Test handshake fails when cartridge HELLO is missing required manifest | bifaci/io_test.go:764 |
| test233 | `Test233_binary_payload_all_byte_values` | TEST233: Test binary payload with all 256 byte values roundtrips through encode/decode | bifaci/io_test.go:798 |
| test234 | `Test234_decode_garbage_bytes` | TEST234: Test decode_frame handles garbage CBOR bytes gracefully with an error | bifaci/io_test.go:823 |
| test235 | `Test235_response_chunk_fields` | TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly | bifaci/host_test.go:10 |
| test236 | `Test236_response_chunk_all_fields_populated` | TEST236: Test ResponseChunk with all fields populated preserves offset, len, and eof | bifaci/host_test.go:34 |
| test237 | `Test237_cartridge_response_single_final_payload` | TEST237: Test CartridgeResponse::Single final_payload returns the single payload slice | bifaci/host_test.go:56 |
| test238 | `Test238_cartridge_response_single_empty_payload` | TEST238: Test CartridgeResponse::Single with empty payload returns empty slice and empty vec | bifaci/host_test.go:68 |
| test239 | `Test239_cartridge_response_streaming_concatenated` | TEST239: Test CartridgeResponse::Streaming concatenated joins all chunk payloads in order | bifaci/host_test.go:79 |
| test240 | `Test240_cartridge_response_streaming_final_payload` | TEST240: Test CartridgeResponse::Streaming final_payload returns the last chunk's payload | bifaci/host_test.go:96 |
| test241 | `Test241_cartridge_response_streaming_empty_chunks` | TEST241: Test CartridgeResponse::Streaming with empty chunks vec returns empty concatenation | bifaci/host_test.go:113 |
| test242 | `Test242_cartridge_response_streaming_preallocation` | TEST242: Test CartridgeResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads | bifaci/host_test.go:127 |
| test243 | `Test243_host_error_variants` | TEST243: Test AsyncHostError variants display correct error messages | bifaci/host_test.go:146 |
| test244 | `Test244_host_error_conversion` | TEST244: Test AsyncHostError::from converts CborError to Cbor variant | bifaci/host_test.go:195 |
| test245 | `Test245_host_error_io_variant` | TEST245: Test AsyncHostError::from converts io::Error to Io variant | bifaci/host_test.go:206 |
| test246 | `Test246_response_chunk_copy` | TEST246: Test AsyncHostError Clone implementation produces equal values | bifaci/host_test.go:217 |
| test247 | `Test247_response_chunk_clone` | TEST247: Test ResponseChunk Clone produces independent copy with same data | bifaci/host_test.go:241 |
| test248 | `Test248_register_and_find_handler` | TEST248: Test register_op and find_handler by exact cap URN | bifaci/cartridge_runtime_test.go:100 |
| test249 | `Test249_raw_handler` | TEST249: Test register_op handler echoes bytes directly | bifaci/cartridge_runtime_test.go:118 |
| test250 | `Test250_typed_handler_deserialization` | TEST250: Test Op handler collects input and processes it | bifaci/cartridge_runtime_test.go:156 |
| test251 | `Test251_typed_handler_rejects_invalid_json` | TEST251: Test Op handler propagates errors through RuntimeError::Handler | bifaci/cartridge_runtime_test.go:196 |
| test252 | `Test252_find_handler_unknown_cap` | TEST252: Test find_handler returns None for unregistered cap URNs | bifaci/cartridge_runtime_test.go:225 |
| test253 | `Test253_handler_is_send_sync` | TEST253: Test OpFactory can be cloned via Arc and sent across tasks (Send + Sync) | bifaci/cartridge_runtime_test.go:238 |
| test254 | `Test254_no_peer_invoker` | TEST254: Test NoPeerInvoker always returns PeerRequest error | bifaci/cartridge_runtime_test.go:276 |
| test255 | `Test255_no_peer_invoker_with_arguments` | TEST255: Test NoPeerInvoker call_with_bytes also returns error | bifaci/cartridge_runtime_test.go:288 |
| test256 | `Test256_new_cartridge_runtime_with_valid_json` | TEST256: Test CartridgeRuntime::with_manifest_json stores manifest data and parses when valid | bifaci/cartridge_runtime_test.go:300 |
| test257 | `Test257_new_cartridge_runtime_with_invalid_json` | TEST257: Test CartridgeRuntime::new with invalid JSON still creates runtime (manifest is None) | bifaci/cartridge_runtime_test.go:315 |
| test258 | `Test258_new_cartridge_runtime_with_manifest_struct` | TEST258: Test CartridgeRuntime::with_manifest creates runtime with valid manifest data | bifaci/cartridge_runtime_test.go:330 |
| test259 | `Test259_extract_effective_payload_non_cbor` | TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged | bifaci/cartridge_runtime_test.go:350 |
| test260 | `Test260_extract_effective_payload_no_content_type` | TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged | bifaci/cartridge_runtime_test.go:363 |
| test261 | `Test261_extract_effective_payload_cbor_match` | TEST261: Test extract_effective_payload with CBOR content extracts matching argument value | bifaci/cartridge_runtime_test.go:376 |
| test262 | `Test262_extract_effective_payload_cbor_no_match` | TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input | bifaci/cartridge_runtime_test.go:421 |
| test263 | `Test263_extract_effective_payload_invalid_cbor` | TEST263: Test extract_effective_payload with invalid CBOR bytes returns deserialization error | bifaci/cartridge_runtime_test.go:444 |
| test264 | `Test264_extract_effective_payload_cbor_not_array` | TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error | bifaci/cartridge_runtime_test.go:453 |
| test0265 | `Test0265_CartridgeErrorResponse` | Mirror-specific coverage: Test cartridge ERR frame is received by host as error | bifaci/integration_test.go:504 |
| test0267 | `Test0267_HeartbeatDuringStreaming` | Mirror-specific coverage: Test cartridge-initiated heartbeat mid-stream is handled transparently by host | bifaci/integration_test.go:849 |
| test0269 | `Test0269_ArgumentsRoundtrip` | Mirror-specific coverage: Test host call with unified CBOR arguments sends correct content_type and payload | bifaci/integration_test.go:1029 |
| test270 | `Test270_multiple_handlers` | TEST270: Test registering multiple Op handlers for different caps and finding each independently | bifaci/cartridge_runtime_test.go:467 |
| test271 | `Test271_handler_replacement` | TEST271: Test Op handler replacing an existing registration for the same cap URN | bifaci/cartridge_runtime_test.go:517 |
| test272 | `Test272_extract_effective_payload_multiple_args` | TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one | bifaci/cartridge_runtime_test.go:544 |
| test273 | `Test273_ExtractEffectivePayloadBinaryValue` | TEST273: Test extract_effective_payload with binary data in CBOR value (not just text) | bifaci/cartridge_runtime_test.go:613 |
| test274 | `Test274_CapArgumentValueNew` | TEST274: Test CapArgumentValue::new stores media_urn and raw byte value | cap/caller_test.go:104 |
| test275 | `Test275_CapArgumentValueFromStr` | TEST275: Test CapArgumentValue::from_str converts string to UTF-8 bytes | cap/caller_test.go:111 |
| test276 | `Test276_CapArgumentValueAsStrValid` | TEST276: Test CapArgumentValue::value_as_str succeeds for UTF-8 data | cap/caller_test.go:118 |
| test277 | `Test277_CapArgumentValueAsStrInvalidUtf8` | TEST277: Test CapArgumentValue::value_as_str fails for non-UTF-8 binary data | cap/caller_test.go:126 |
| test278 | `Test278_CapArgumentValueEmpty` | TEST278: Test CapArgumentValue::new with empty value stores empty vec | cap/caller_test.go:133 |
| test279 | `Test279_CapArgumentValueClone` | TEST279: Test CapArgumentValue Clone produces independent copy with same data | cap/caller_test.go:142 |
| test280 | `Test280_CapArgumentValueDebug` | TEST280: Test CapArgumentValue Debug format includes media_urn and value | cap/caller_test.go:150 |
| test281 | `Test281_CapArgumentValueIntoString` | TEST281: Test CapArgumentValue::new accepts Into<String> for media_urn (String and &str) | cap/caller_test.go:157 |
| test282 | `Test282_CapArgumentValueUnicode` | TEST282: Test CapArgumentValue::from_str with Unicode string preserves all characters | cap/caller_test.go:167 |
| test283 | `Test283_CapArgumentValueLargeBinary` | TEST283: Test CapArgumentValue with large binary payload preserves all bytes | cap/caller_test.go:175 |
| test284 | `Test284_HandshakeHostCartridge` | TEST284: Handshake exchanges HELLO frames, negotiates limits | bifaci/integration_test.go:264 |
| test285 | `Test285_RequestResponseSimple` | TEST285: Simple request-response flow (REQ → END with payload) | bifaci/integration_test.go:306 |
| test286 | `Test286_StreamingChunks` | TEST286: Streaming response with multiple CHUNK frames | bifaci/integration_test.go:368 |
| test287 | `Test287_HeartbeatFromHost` | TEST287: Host-initiated heartbeat | bifaci/integration_test.go:446 |
| test288 | `Test288_media_documentation_propagates_through_resolve` | TEST288: Documentation propagates from MediaDef through ResolveMediaUrn into ResolvedMediaDef. Verifies description and documentation remain distinct. | media/spec_test.go:594 |
| test0289 | `Test0289_media_def_def_documentation_round_trip` | TEST289: MediaDef serializes documentation only when present and round-trips losslessly. Mirrors TEST1127/1128 for the cap side. | media/spec_test.go:618 |
| test290 | `Test290_LimitsNegotiation` | TEST290: Limit negotiation picks minimum | bifaci/integration_test.go:637 |
| test291 | `Test291_BinaryPayloadRoundtrip` | TEST291: Binary payload roundtrip (all 256 byte values) | bifaci/integration_test.go:677 |
| test292 | `Test292_MessageIdUniqueness` | TEST292: Sequential requests get distinct MessageIds | bifaci/integration_test.go:751 |
| test293 | `Test293_CartridgeRuntimeHandlerRegistration` | TEST293: Test CartridgeRuntime Op registration and lookup by exact and non-existent cap URN | bifaci/integration_test.go:822 |
| test299 | `Test299_EmptyPayloadRoundtrip` | TEST299: Empty payload request/response roundtrip | bifaci/integration_test.go:1158 |
| test300 | `Test300_get_cartridge_by_id_channel_isolation` | TEST300: A cartridge with the same id can independently exist in both channels. Each lookup must return the channel-specific entry. | bifaci/cartridge_repo_test.go:845 |
| test301 | `Test301_transform_walks_both_channels_release_first` | TEST301: Walking both channels produces release entries first. | bifaci/cartridge_repo_test.go:894 |
| test304 | `Test304_media_availability_output_constant` | TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags | urn/media_urn_test.go:356 |
| test305 | `Test305_media_path_output_constant` | TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags | urn/media_urn_test.go:364 |
| test306 | `Test306_availability_and_path_output_distinct` | TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs | urn/media_urn_test.go:372 |
| test307 | `Test307_model_availability_urn` | TEST307: Test model_availability_urn builds valid cap URN with correct op and media defs | standard/caps_test.go:11 |
| test308 | `Test308_model_path_urn` | TEST308: Test model_path_urn builds valid cap URN with correct op and media defs | standard/caps_test.go:19 |
| test309 | `Test309_model_availability_and_path_are_distinct` | TEST309: Test model_availability_urn and model_path_urn produce distinct URNs | standard/caps_test.go:27 |
| test310 | `Test310_llm_generate_text_urn_shape` | TEST310: llm_generate_text_urn() produces a valid cap URN with a UTF-8 text input and plain-text terminal output. | standard/caps_test.go:35 |
| test312 | `Test312_all_urn_builders_produce_valid_urns` | TEST312: Test all URN builders produce parseable cap URNs | standard/caps_test.go:46 |
| test319 | `Test319_update_cache_rejects_malformed_cap_urn` | TEST319: A registry response with a malformed cap URN inside cap_groups must propagate as ParseError when indexed into the cache, not silently disappear. | bifaci/cartridge_repo_test.go:769 |
| test320 | `Test320_cartridge_info_construction` | TEST320-335: CartridgeRepoServer and CartridgeRepoClient tests | bifaci/cartridge_repo_test.go:64 |
| test321 | `Test321_cartridge_info_is_signed` | TEST321: CartridgeInfo.is_signed() returns true when signature (team_id + signed_at) is present, false when either is empty. | bifaci/cartridge_repo_test.go:94 |
| test322 | `Test322_cartridge_info_build_for_platform` | TEST322: CartridgeInfo.build_for_platform() returns the build that matches the requested platform string and None otherwise. | bifaci/cartridge_repo_test.go:121 |
| test323 | `Test323_cartridge_repo_server_validate_registry` | TEST323: CartridgeRepoServer requires schema 5.0 and rejects older. | bifaci/cartridge_repo_test.go:177 |
| test324 | `Test324_cartridge_repo_server_transform_to_array` | TEST324: CartridgeRepoServer transforms a v4.0 entry into a flat CartridgeInfo, preserving cap_groups verbatim. | bifaci/cartridge_repo_test.go:205 |
| test325 | `Test325_cartridge_repo_server_get_cartridges` | TEST325: CartridgeRepoServer.get_cartridges() returns all parsed cartridges | bifaci/cartridge_repo_test.go:254 |
| test326 | `Test326_cartridge_repo_server_get_cartridge_by_id` | TEST326: CartridgeRepoServer.get_cartridge() returns cartridge matching the given ID | bifaci/cartridge_repo_test.go:284 |
| test327 | `Test327_cartridge_repo_server_search_cartridges` | TEST327: CartridgeRepoServer.search_cartridges() filters by text query against name and description | bifaci/cartridge_repo_test.go:335 |
| test328 | `Test328_cartridge_repo_server_get_by_category` | TEST328: CartridgeRepoServer.get_by_category() filters cartridges by category tag | bifaci/cartridge_repo_test.go:374 |
| test329 | `Test329_cartridge_repo_server_get_by_cap` | TEST329: CartridgeRepoServer.get_suggestions_for_cap() finds cartridges providing a given cap URN | bifaci/cartridge_repo_test.go:413 |
| test330 | `Test330_cartridge_repo_client_update_cache` | TEST330: CartridgeRepoClient updates its local cache, keyed by (channel, id) so the same id can independently coexist in both channels. | bifaci/cartridge_repo_test.go:474 |
| test331 | `Test331_cartridge_repo_client_get_suggestions` | TEST331: CartridgeRepoClient.GetSuggestionsForCap() returns cartridge suggestions and propagates the source channel onto each suggestion. | bifaci/cartridge_repo_test.go:511 |
| test332 | `Test332_cartridge_repo_client_get_cartridge` | TEST332: CartridgeRepoClient.GetCartridge() retrieves by (channel, id). | bifaci/cartridge_repo_test.go:568 |
| test333 | `Test333_cartridge_repo_client_get_all_caps` | TEST333: CartridgeRepoClient.get_all_caps() returns aggregate cap URNs from all cached cartridges | bifaci/cartridge_repo_test.go:603 |
| test334 | `Test334_cartridge_repo_client_needs_sync` | TEST334: CartridgeRepoClient.needs_sync() returns true when cache TTL has expired | bifaci/cartridge_repo_test.go:668 |
| test335 | `Test335_cartridge_repo_server_client_integration` | TEST335: Server creates registry response and client consumes it end-to-end | bifaci/cartridge_repo_test.go:687 |
| test336 | `Test336_FilePathReadsFilePassesBytes` | TEST336: Single file-path arg with stdin source reads file and passes bytes to handler | bifaci/cartridge_runtime_test.go:859 |
| test337 | `Test337_FilePathWithoutStdinPassesString` | TEST337: file-path arg without stdin source passes path as string (no conversion) | bifaci/cartridge_runtime_test.go:938 |
| test338 | `Test338_FilePathViaCliFlag` | TEST338: file-path arg reads file via --file CLI flag | bifaci/cartridge_runtime_test.go:979 |
| test339 | `Test339_FilePathArrayGlobExpansion` | TEST339: file-path arg with is_sequence=true expands a glob to N files and the runtime delivers them as a CBOR Array of Bytes — one array item per matched file. List-ness comes from the arg declaration, not from any `;list` URN tag. Mirrors Rust test339_file_path_array_glob_expansion. | bifaci/cartridge_runtime_test.go:1026 |
| test340 | `Test340_FileNotFoundClearError` | TEST340: File not found error provides clear message | bifaci/cartridge_runtime_test.go:1110 |
| test341 | `Test341_StdinPrecedenceOverFilePath` | TEST341: stdin takes precedence over file-path in source order | bifaci/cartridge_runtime_test.go:1152 |
| test342 | `Test342_FilePathPositionZeroReadsFirstArg` | TEST342: file-path with position 0 reads first positional arg as file | bifaci/cartridge_runtime_test.go:1195 |
| test343 | `Test343_NonFilePathArgsUnaffected` | TEST343: Non-file-path args are not affected by file reading | bifaci/cartridge_runtime_test.go:1252 |
| test346 | `Test346_LargeFileReadsSuccessfully` | TEST346: Large file (1MB) reads successfully | bifaci/cartridge_runtime_test.go:1377 |
| test347 | `Test347_EmptyFileReadsAsEmptyBytes` | TEST347: Empty file reads as empty bytes | bifaci/cartridge_runtime_test.go:1425 |
| test348 | `Test348_FilePathConversionRespectsSourceOrder` | TEST348: file-path conversion respects source order | bifaci/cartridge_runtime_test.go:1469 |
| test349 | `Test349_FilePathMultipleSourcesFallback` | TEST349: file-path arg with multiple sources tries all in order | bifaci/cartridge_runtime_test.go:1516 |
| test350 | `Test350_FullCLIModeWithFilePathIntegration` | TEST350: Integration test - full CLI mode invocation with file-path | bifaci/cartridge_runtime_test.go:1562 |
| test352 | `Test352_FilePermissionDeniedClearError` | TEST352: file permission denied error is clear (Unix-specific) | bifaci/cartridge_runtime_test.go:1696 |
| test353 | `Test353_CBORPayloadFormatConsistency` | TEST353: CBOR payload format matches between CLI and CBOR mode | bifaci/cartridge_runtime_test.go:1749 |
| test354 | `Test354_GlobPatternNoMatchesEmptyArray` | TEST354: Glob pattern with no matches fails hard (NO FALLBACK) | bifaci/cartridge_runtime_test.go:1807 |
| test355 | `Test355_GlobPatternSkipsDirectories` | TEST355: Glob pattern skips directories. Mirrors Rust test355_glob_pattern_skips_directories. | bifaci/cartridge_runtime_test.go:1852 |
| test356 | `Test356_MultipleGlobPatternsCombined` | TEST356: Multiple glob patterns combined | bifaci/cartridge_runtime_test.go:1924 |
| test357 | `Test357_SymlinksFollowed` | TEST357: Symlinks are followed when reading files | bifaci/cartridge_runtime_test.go:2003 |
| test358 | `Test358_BinaryFileNonUTF8` | TEST358: Binary file with non-UTF8 data reads correctly | bifaci/cartridge_runtime_test.go:2060 |
| test359 | `Test359_InvalidGlobPatternFails` | TEST359: Invalid glob pattern fails with clear error | bifaci/cartridge_runtime_test.go:2110 |
| test360 | `Test360_ExtractEffectivePayloadWithFileData` | TEST360: Extract effective payload handles file-path data correctly | bifaci/cartridge_runtime_test.go:2149 |
| test361 | `Test361_CLIModeFilePath` | TEST361: CLI mode with file path - pass file path as command-line argument | bifaci/cartridge_runtime_test.go:2231 |
| test362 | `Test362_CLIModePipedBinary` | TEST362: CLI mode with binary piped in - pipe binary data via stdin This test simulates real-world conditions: - Pure binary data piped to stdin (NOT CBOR) - CLI mode detected (command arg present) - Cap accepts stdin source - Binary is chunked on-the-fly and accumulated - Handler receives complete CBOR payload | bifaci/cartridge_runtime_test.go:2280 |
| test363 | `Test363_CBORModeChunkedContent` | TEST363: CBOR mode with chunked content - send file content streaming as chunks | bifaci/cartridge_runtime_test.go:2367 |
| test364 | `Test364_CBORModeFilePath` | TEST364: CBOR mode with file path - send file path in CBOR arguments (auto-conversion) | bifaci/cartridge_runtime_test.go:2510 |
| test365 | `Test365_stream_start_frame` | TEST365: Frame::stream_start stores request_id, stream_id, and media_urn | bifaci/frame_test.go:652 |
| test366 | `Test366_stream_end_frame` | TEST366: Frame::stream_end stores request_id and stream_id | bifaci/frame_test.go:674 |
| test367 | `Test367_stream_start_with_empty_stream_id` | TEST367: StreamStart frame with empty stream_id still constructs (validation happens elsewhere) | bifaci/frame_test.go:695 |
| test368 | `Test368_stream_start_with_empty_media_urn` | TEST368: StreamStart frame with empty media_urn still constructs (validation happens elsewhere) | bifaci/frame_test.go:714 |
| test389 | `Test389_stream_start_roundtrip` | TEST389: StreamStart encode/decode roundtrip preserves stream_id and media_urn | bifaci/io_test.go:832 |
| test390 | `Test390_stream_end_roundtrip` | TEST390: StreamEnd encode/decode roundtrip preserves stream_id, no media_urn | bifaci/io_test.go:860 |
| test395 | `Test395_BuildPayloadSmall` | TEST395: Small payload (< max_chunk) produces correct CBOR arguments | bifaci/cartridge_runtime_test.go:2565 |
| test396 | `Test396_BuildPayloadLarge` | TEST396: Large payload (> max_chunk) accumulates across chunks correctly | bifaci/cartridge_runtime_test.go:2614 |
| test397 | `Test397_BuildPayloadEmpty` | TEST397: Empty reader produces valid empty CBOR arguments | bifaci/cartridge_runtime_test.go:2658 |
| test398 | `Test398_BuildPayloadIOError` | TEST398: IO error from reader propagates as RuntimeError::Io | bifaci/cartridge_runtime_test.go:2701 |
| test399 | `Test399_relay_notify_discriminant_roundtrip` | TEST399: Verify RelayNotify frame type discriminant roundtrips through u8 (value 10) | bifaci/frame_test.go:733 |
| test400 | `Test400_relay_state_discriminant_roundtrip` | TEST400: Verify RelayState frame type discriminant roundtrips through u8 (value 11) | bifaci/frame_test.go:746 |
| test401 | `Test401_relay_notify_factory_and_accessors` | TEST401: Verify relay_notify factory stores manifest and limits, and accessors extract them | bifaci/frame_test.go:759 |
| test402 | `Test402_relay_state_factory_and_payload` | TEST402: Verify relay_state factory stores resource payload in frame payload field | bifaci/frame_test.go:802 |
| test403 | `Test403_frame_type_one_past_cancel` | TEST403: Verify from_u8 returns None for values past the last valid frame type | bifaci/frame_test.go:816 |
| test404 | `Test404_slave_sends_relay_notify_on_connect` | TEST404: Slave sends RelayNotify on connect (initial_notify parameter) | bifaci/relay_test.go:15 |
| test405 | `Test405_master_reads_relay_notify` | TEST405: Master reads RelayNotify and extracts manifest + limits | bifaci/relay_test.go:70 |
| test406 | `Test406_slave_stores_relay_state` | TEST406: Slave stores RelayState from master | bifaci/relay_test.go:112 |
| test407 | `Test407_protocol_frames_pass_through` | TEST407: Protocol frames pass through slave transparently (both directions) | bifaci/relay_test.go:166 |
| test408 | `Test408_relay_frames_not_forwarded` | TEST408: RelayNotify/RelayState are NOT forwarded through relay | bifaci/relay_test.go:293 |
| test409 | `Test409_slave_injects_relay_notify_midstream` | TEST409: Slave can inject RelayNotify mid-stream (cap change) | bifaci/relay_test.go:374 |
| test410 | `Test410_master_receives_updated_relay_notify` | TEST410: Master receives updated RelayNotify (cap change callback via read_frame) | bifaci/relay_test.go:449 |
| test411 | `Test411_socket_close_detection` | TEST411: Socket close detection (both directions) | bifaci/relay_test.go:543 |
| test412 | `Test412_bidirectional_concurrent_flow` | TEST412: Bidirectional concurrent frame flow through relay | bifaci/relay_test.go:589 |
| test413 | `Test413_register_cartridge_adds_cap_table` | TEST413: Register cartridge adds entries to cap_table | bifaci/host_multi_test.go:52 |
| test415 | `Test415_req_triggers_spawn` | TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary) | bifaci/host_multi_test.go:81 |
| test416 | `Test416_attach_cartridge_handshake` | TEST416: Attach cartridge performs HELLO handshake, extracts manifest, updates capabilities | bifaci/host_multi_test.go:127 |
| test417 | `Test417_route_req_by_cap_urn` | TEST417: Route REQ to correct cartridge by cap_urn (with two attached cartridges) | bifaci/host_multi_test.go:167 |
| test418 | `Test418_route_continuation_by_req_id` | TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn) Verifies that after the initial REQ→cartridge routing, all subsequent continuation frames with the same req_id are routed to the same cartridge — even though no cap_urn is present on those frames. | bifaci/host_multi_test.go:274 |
| test419 | `Test419_heartbeat_local_handling` | TEST419: Cartridge HEARTBEAT handled locally (not forwarded to relay) | bifaci/host_multi_test.go:365 |
| test420 | `Test420_cartridge_frames_forwarded_to_relay` | TEST420: Cartridge non-HELLO/non-HB frames forwarded to relay (pass-through) | bifaci/host_multi_test.go:446 |
| test421 | `Test421_cartridge_death_updates_caps` | TEST421: Cartridge death updates capability list (caps removed) | bifaci/host_multi_test.go:541 |
| test422 | `Test422_cartridge_death_sends_err` | TEST422: Cartridge death sends ERR for all pending requests via relay | bifaci/host_multi_test.go:596 |
| test423 | `Test423_multi_cartridge_distinct_caps` | TEST423: Multiple cartridges registered with distinct caps route independently | bifaci/host_multi_test.go:670 |
| test424 | `Test424_concurrent_requests_same_cartridge` | TEST424: Concurrent requests to the same cartridge are handled independently | bifaci/host_multi_test.go:801 |
| test425 | `Test425_find_cartridge_for_cap_unknown` | TEST425: find_cartridge_for_cap returns None for unregistered cap | bifaci/host_multi_test.go:919 |
| test426 | `Test426_relay_switch_single_master_req_response` | TEST426: Single master REQ/response routing | bifaci/relay_switch_test.go:81 |
| test427 | `Test427_relay_switch_multi_master_cap_routing` | TEST427: Multi-master cap routing | bifaci/relay_switch_test.go:145 |
| test428 | `Test428_relay_switch_unknown_cap_returns_error` | TEST428: Unknown cap returns error | bifaci/relay_switch_test.go:229 |
| test429 | `Test429_relay_switch_find_master_for_cap` | TEST429: Cap routing logic (find_master_for_cap) | bifaci/relay_switch_test.go:272 |
| test430 | `Test430_relay_switch_tie_breaking` | TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent) | bifaci/relay_switch_test.go:358 |
| test431 | `Test431_relay_switch_continuation_frame_routing` | TEST431: Continuation frame routing (CHUNK, END follow REQ) | bifaci/relay_switch_test.go:429 |
| test432 | `Test432_relay_switch_empty_masters_list_error` | TEST432: Empty masters list creates empty switch, add_master works | bifaci/relay_switch_test.go:504 |
| test433 | `Test433_relay_switch_capability_aggregation_deduplicates` | TEST433: Capability aggregation deduplicates caps | bifaci/relay_switch_test.go:519 |
| test434 | `Test434_relay_switch_limits_negotiation_minimum` | TEST434: Limits negotiation takes minimum | bifaci/relay_switch_test.go:582 |
| test435 | `Test435_relay_switch_urn_matching` | TEST435: URN matching (exact vs accepts()) | bifaci/relay_switch_test.go:631 |
| test436 | `Test436_compute_checksum` | TEST436: Verify FNV-1a checksum function produces consistent results | bifaci/frame_test.go:862 |
| test437 | `Test437_preferred_cap_routes_to_generic` | TEST437: find_master_for_cap with preferred_cap routes to generic handler With is_dispatchable semantics: - Generic provider (in=media:) CAN dispatch specific request (in="media:ext=pdf") because media: (wildcard) accepts any input type - Preference routes to preferred among dispatchable candidates | bifaci/relay_switch_test.go:690 |
| test438 | `Test438_preferred_cap_falls_back_when_not_comparable` | TEST438: find_master_for_cap with preference falls back to closest-specificity when preferred cap is not in the comparable set | bifaci/relay_switch_test.go:754 |
| test439 | `Test439_generic_provider_can_dispatch_specific_request` | TEST439: Generic provider CAN dispatch specific request (but only matches if no more specific provider exists) With is_dispatchable: generic provider (in=media:) CAN handle specific request (in="media:ext=pdf") because media: accepts any input type. With preference, can route to generic even when more specific exists. | bifaci/relay_switch_test.go:792 |
| test440 | `Test440_chunk_index_checksum_roundtrip` | TEST440: CHUNK frame with chunk_index and checksum roundtrips through encode/decode | bifaci/io_test.go:951 |
| test441 | `Test441_stream_end_chunk_count_roundtrip` | TEST441: STREAM_END frame with chunk_count roundtrips through encode/decode | bifaci/io_test.go:992 |
| test442 | `Test442_seq_assigner_monotonic_same_rid` | TEST442: SeqAssigner assigns seq 0,1,2,3 for consecutive frames with same RID | bifaci/frame_test.go:879 |
| test443 | `Test443_seq_assigner_independent_rids` | TEST443: SeqAssigner maintains independent counters for different RIDs | bifaci/frame_test.go:908 |
| test444 | `Test444_seq_assigner_skips_non_flow` | TEST444: SeqAssigner skips non-flow frames (Heartbeat, RelayNotify, RelayState, Hello) | bifaci/frame_test.go:934 |
| test445 | `Test445_seq_assigner_remove_by_flow_key` | TEST445: SeqAssigner.remove with FlowKey(rid, None) resets that flow; FlowKey(rid, Some(xid)) is unaffected | bifaci/frame_test.go:962 |
| test446 | `Test446_seq_assigner_mixed_types` | TEST446: SeqAssigner handles mixed frame types (REQ, CHUNK, LOG, END) for same RID | bifaci/frame_test.go:1044 |
| test447 | `Test447_flow_key_with_xid` | TEST447: FlowKey::from_frame extracts (rid, Some(xid)) when routing_id present | bifaci/frame_test.go:1065 |
| test448 | `Test448_flow_key_without_xid` | TEST448: FlowKey::from_frame extracts (rid, None) when routing_id absent | bifaci/frame_test.go:1082 |
| test449 | `Test449_flow_key_equality` | TEST449: FlowKey equality: same rid+xid equal, different xid different key | bifaci/frame_test.go:1096 |
| test450 | `Test450_flow_key_hash_lookup` | TEST450: FlowKey hash: same keys hash equal (HashMap lookup) | bifaci/frame_test.go:1118 |
| test451 | `Test451_reorder_buffer_in_order` | TEST451: ReorderBuffer in-order delivery: seq 0,1,2 delivered immediately | bifaci/frame_test.go:1132 |
| test452 | `Test452_reorder_buffer_out_of_order` | TEST452: ReorderBuffer out-of-order: seq 1 then 0 delivers both in order | bifaci/frame_test.go:1157 |
| test453 | `Test453_reorder_buffer_gap_fill` | TEST453: ReorderBuffer gap fill: seq 0,2,1 delivers 0, buffers 2, then delivers 1+2 | bifaci/frame_test.go:1180 |
| test454 | `Test454_reorder_buffer_stale_seq` | TEST454: ReorderBuffer stale seq is hard error | bifaci/frame_test.go:1207 |
| test455 | `Test455_reorder_buffer_overflow` | TEST455: ReorderBuffer overflow triggers protocol error | bifaci/frame_test.go:1228 |
| test456 | `Test456_reorder_buffer_independent_flows` | TEST456: Multiple concurrent flows reorder independently | bifaci/frame_test.go:1248 |
| test457 | `Test457_reorder_buffer_cleanup` | TEST457: cleanup_flow removes state; new frames start at seq 0 | bifaci/frame_test.go:1276 |
| test458 | `Test458_reorder_buffer_non_flow_bypass` | TEST458: Non-flow frames bypass reorder entirely | bifaci/frame_test.go:1301 |
| test459 | `Test459_reorder_buffer_end_frame` | TEST459: Terminal END frame flows through correctly | bifaci/frame_test.go:1317 |
| test460 | `Test460_reorder_buffer_err_frame` | TEST460: Terminal ERR frame flows through correctly | bifaci/frame_test.go:1975 |
| test472 | `Test472_handshake_negotiates_reorder_buffer` | TEST472: Handshake negotiates max_reorder_buffer (minimum of both sides) | bifaci/io_test.go:1211 |
| test473 | `Test473_cap_discard_parses_as_valid_urn` | TEST473: CAP_DISCARD parses as valid CapUrn with in=media: and out=media:void | standard/caps_test.go:59 |
| test474 | `Test474_cap_discard_structure` | TEST474: CAP_DISCARD accepts specific-input/void-output caps | standard/caps_test.go:68 |
| test475 | `Test475_validate_passes_with_identity` | TEST475: validate() passes with CAP_IDENTITY in a cap group | bifaci/manifest_test.go:325 |
| test476 | `Test476_validate_fails_without_identity` | TEST476: validate() fails without CAP_IDENTITY | bifaci/manifest_test.go:336 |
| test478 | `Test478_auto_registers_identity_handler` | TEST478: CartridgeRuntime auto-registers identity and discard handlers on construction | bifaci/cartridge_runtime_test.go:3375 |
| test479 | `Test479_custom_identity_overrides_default` | TEST479: Custom identity Op overrides auto-registered default | bifaci/cartridge_runtime_test.go:3400 |
| test491 | `Test491_chunk_requires_chunk_index_and_checksum` | TEST491: Frame::chunk constructor requires and sets chunk_index and checksum | bifaci/frame_test.go:1370 |
| test492 | `Test492_stream_end_requires_chunk_count` | TEST492: Frame::stream_end constructor requires and sets chunk_count | bifaci/frame_test.go:1386 |
| test493 | `Test493_compute_checksum_fnv1a_test_vectors` | TEST493: compute_checksum produces correct FNV-1a hash for known test vectors | bifaci/frame_test.go:1399 |
| test494 | `Test494_compute_checksum_deterministic` | TEST494: compute_checksum is deterministic | bifaci/frame_test.go:1406 |
| test495 | `Test495_cbor_rejects_chunk_without_chunk_index` | TEST495: CBOR decode REJECTS CHUNK frame missing chunk_index field | bifaci/frame_test.go:1417 |
| test496 | `Test496_cbor_rejects_chunk_without_checksum` | TEST496: CBOR decode REJECTS CHUNK frame missing checksum field | bifaci/frame_test.go:1445 |
| test497 | `Test497_chunk_corrupted_payload_rejected` | TEST497: Verify CHUNK frame with corrupted payload is rejected by checksum | bifaci/io_test.go:1022 |
| test498 | `Test498_routing_id_cbor_roundtrip` | TEST498: routing_id field roundtrips through CBOR encoding | bifaci/frame_test.go:1473 |
| test499 | `Test499_chunk_index_checksum_cbor_roundtrip` | TEST499: chunk_index and checksum roundtrip through CBOR encoding | bifaci/frame_test.go:1491 |
| test500 | `Test500_chunk_count_cbor_roundtrip` | TEST500: chunk_count roundtrips through CBOR encoding | bifaci/frame_test.go:1511 |
| test501 | `Test501_frame_new_initializes_optional_fields_none` | TEST501: Frame::new initializes new fields to None | bifaci/frame_test.go:1528 |
| test503 | `Test503_compute_checksum_empty_data` | TEST503: compute_checksum handles empty data correctly | bifaci/frame_test.go:1546 |
| test504 | `Test504_compute_checksum_large_payload` | TEST504: compute_checksum handles large payloads without overflow | bifaci/frame_test.go:1552 |
| test505 | `Test505_chunk_with_offset_sets_chunk_index` | TEST505: chunk_with_offset sets chunk_index correctly | bifaci/frame_test.go:1565 |
| test506 | `Test506_compute_checksum_different_data_different_hash` | TEST506: Different data produces different checksums | bifaci/frame_test.go:1582 |
| test507 | `Test507_reorder_buffer_xid_isolation` | TEST507: ReorderBuffer isolates flows by XID (routing_id) - same RID different XIDs | bifaci/frame_test.go:1589 |
| test508 | `Test508_reorder_buffer_duplicate_buffered_seq` | TEST508: ReorderBuffer rejects duplicate seq already in buffer | bifaci/frame_test.go:1620 |
| test509 | `Test509_reorder_buffer_large_gap_rejected` | TEST509: ReorderBuffer handles large seq gaps without DOS | bifaci/frame_test.go:1636 |
| test510 | `Test510_reorder_buffer_multiple_gaps` | TEST510: ReorderBuffer with multiple interleaved gaps fills correctly | bifaci/frame_test.go:1655 |
| test511 | `Test511_reorder_buffer_cleanup_with_buffered_frames` | TEST511: ReorderBuffer cleanup with buffered frames discards them | bifaci/frame_test.go:1689 |
| test512 | `Test512_reorder_buffer_burst_delivery` | TEST512: ReorderBuffer delivers burst of consecutive buffered frames | bifaci/frame_test.go:1713 |
| test513 | `Test513_reorder_buffer_mixed_types_same_flow` | TEST513: ReorderBuffer different frame types in same flow maintain order | bifaci/frame_test.go:1734 |
| test514 | `Test514_reorder_buffer_xid_cleanup_isolation` | TEST514: ReorderBuffer with XID cleanup doesn't affect different XID | bifaci/frame_test.go:1757 |
| test515 | `Test515_reorder_buffer_overflow_error_details` | TEST515: ReorderBuffer overflow error includes diagnostic information | bifaci/frame_test.go:1783 |
| test516 | `Test516_reorder_buffer_stale_error_details` | TEST516: ReorderBuffer stale error includes diagnostic information | bifaci/frame_test.go:1799 |
| test517 | `Test517_flow_key_none_vs_some_xid` | TEST517: FlowKey with None XID differs from Some(xid) | bifaci/frame_test.go:1817 |
| test518 | `Test518_reorder_buffer_empty_ready_vec` | TEST518: ReorderBuffer handles zero-length ready vec correctly | bifaci/frame_test.go:1832 |
| test519 | `Test519_reorder_buffer_state_persistence` | TEST519: ReorderBuffer state persists across accept calls | bifaci/frame_test.go:1844 |
| test520 | `Test520_reorder_buffer_per_flow_limit` | TEST520: ReorderBuffer max_buffer_per_flow is per-flow not global | bifaci/frame_test.go:1863 |
| test521 | `Test521_relay_notify_cbor_roundtrip` | TEST521: RelayNotify CBOR roundtrip preserves manifest and limits | bifaci/frame_test.go:1886 |
| test522 | `Test522_relay_state_cbor_roundtrip` | TEST522: RelayState CBOR roundtrip preserves payload | bifaci/frame_test.go:1906 |
| test523 | `Test523_relay_notify_not_flow_frame` | TEST523: is_flow_frame returns false for RelayNotify | bifaci/frame_test.go:1922 |
| test524 | `Test524_relay_state_not_flow_frame` | TEST524: is_flow_frame returns false for RelayState | bifaci/frame_test.go:1928 |
| test525 | `Test525_relay_notify_empty_manifest` | TEST525: RelayNotify with empty manifest is valid | bifaci/frame_test.go:1934 |
| test526 | `Test526_relay_state_empty_payload` | TEST526: RelayState with empty payload is valid | bifaci/frame_test.go:1941 |
| test527 | `Test527_relay_notify_large_manifest` | TEST527: RelayNotify with large manifest roundtrips correctly | bifaci/frame_test.go:1948 |
| test528 | `Test528_relay_frames_use_uint_zero_id` | TEST528: RelayNotify and RelayState use MessageId::Uint(0) | bifaci/frame_test.go:1965 |
| test529 | `Test529_input_stream_recv_order` | TEST529: InputStream recv yields chunks in order | bifaci/cartridge_runtime_test.go:3488 |
| test530 | `Test530_input_stream_collect_bytes` | TEST530: InputStream::collect_bytes concatenates byte chunks | bifaci/cartridge_runtime_test.go:3512 |
| test531 | `Test531_input_stream_collect_bytes_text` | TEST531: InputStream::collect_bytes handles text chunks | bifaci/cartridge_runtime_test.go:3526 |
| test532 | `Test532_input_stream_empty` | TEST532: InputStream empty stream produces empty bytes | bifaci/cartridge_runtime_test.go:3540 |
| test533 | `Test533_input_stream_error_propagation` | TEST533: InputStream propagates errors | bifaci/cartridge_runtime_test.go:3553 |
| test534 | `Test534_input_stream_media_urn` | TEST534: InputStream::media_urn returns correct URN | bifaci/cartridge_runtime_test.go:3575 |
| test535 | `Test535_input_package_iteration` | TEST535: InputPackage recv yields streams | bifaci/cartridge_runtime_test.go:3597 |
| test536 | `Test536_input_package_collect_all_bytes` | TEST536: InputPackage::collect_all_bytes aggregates all streams | bifaci/cartridge_runtime_test.go:3622 |
| test537 | `Test537_input_package_empty` | TEST537: InputPackage empty package produces empty bytes | bifaci/cartridge_runtime_test.go:3640 |
| test538 | `Test538_input_package_error_propagation` | TEST538: InputPackage propagates stream errors | bifaci/cartridge_runtime_test.go:3655 |
| test539 | `Test539_output_stream_sends_stream_start` | TEST539: OutputStream sends STREAM_START on first write | bifaci/cartridge_runtime_test.go:3712 |
| test540 | `Test540_output_stream_close_sends_stream_end` | TEST540: OutputStream::close sends STREAM_END with correct chunk_count | bifaci/cartridge_runtime_test.go:3731 |
| test541 | `Test541_output_stream_chunks_large_data` | TEST541: OutputStream chunks large data correctly | bifaci/cartridge_runtime_test.go:3754 |
| test542 | `Test542_output_stream_empty` | TEST542: OutputStream empty stream sends STREAM_START and STREAM_END only | bifaci/cartridge_runtime_test.go:3778 |
| test544 | `Test544_peer_invoker_sends_end_frame` | TEST544: PeerCall::finish sends END frame | bifaci/cartridge_runtime_test.go:2731 |
| test545 | `Test545_demux_peer_response_returns_data` | TEST545: PeerCall::finish returns PeerResponse with data | bifaci/cartridge_runtime_test.go:2764 |
| test546 | `Test546_is_image` | TEST546: is_image returns true only when image marker tag is present | urn/media_urn_test.go:383 |
| test547 | `Test547_is_audio` | TEST547: is_audio returns true only when audio marker tag is present | urn/media_urn_test.go:411 |
| test548 | `Test548_is_video` | TEST548: is_video returns true only when video marker tag is present | urn/media_urn_test.go:439 |
| test549 | `Test549_is_numeric` | TEST549: is_numeric returns true only when numeric marker tag is present | urn/media_urn_test.go:463 |
| test550 | `Test550_is_bool` | TEST550: is_bool returns true only when bool marker tag is present | urn/media_urn_test.go:495 |
| test551 | `Test551_is_file_path` | TEST551: is_file_path returns true for the single file-path media URN, false for everything else. There is no "array" variant — cardinality is carried by is_sequence on the wire, not by URN tags. | urn/media_urn_test.go:524 |
| test555 | `Test555_with_tag_and_without_tag` | TEST555: with_tag adds a tag and without_tag removes it | urn/media_urn_test.go:674 |
| test556 | `Test556_image_media_urn_for_ext` | TEST556: image_media_urn_for_ext creates valid image media URN | urn/media_urn_test.go:698 |
| test557 | `Test557_audio_media_urn_for_ext` | TEST557: audio_media_urn_for_ext creates valid audio media URN | urn/media_urn_test.go:710 |
| test558 | `Test558_predicate_constant_consistency` | TEST558: predicates are consistent with constants — every constant triggers exactly the expected predicates | urn/media_urn_test.go:540 |
| test559 | `Test559_without_tag` | TEST559: without_tag removes tag, rejects structural keys, case-insensitive for keys | urn/cap_urn_test.go:1035 |
| test560 | `Test560_with_in_out_spec` | TEST560: with_in_spec and with_out_spec change direction specs | urn/cap_urn_test.go:1060 |
| test561 | `Test561_in_out_media_urn` | TEST561: in_media_urn and out_media_urn parse direction specs into MediaUrn | urn/cap_urn_test.go:1436 |
| test562 | `Test562_canonical_option` | TEST562: canonical_option returns None for None input, canonical string for Some | urn/cap_urn_test.go:1463 |
| test563 | `Test563_find_all_matches` | TEST563: CapMatcher::find_all_matches returns all matching caps sorted by specificity | urn/cap_urn_test.go:1084 |
| test564 | `Test564_are_compatible` | TEST564: CapMatcher::are_compatible detects bidirectional overlap | urn/cap_urn_test.go:1111 |
| test565 | `Test565_tags_to_string` | TEST565: tags_to_string returns only tags portion without prefix | urn/cap_urn_test.go:1141 |
| test566 | `Test566_with_tag_ignores_in_out` | TEST566: with_tag rejects structural keys | urn/cap_urn_test.go:1153 |
| test567 | `Test567_str_variants` | TEST567: conforms_to_str and accepts_str work with string arguments | urn/cap_urn_test.go:1164 |
| test568 | `Test568_dispatch_output_tag_order` | TEST568: is_dispatchable with different tag order in output spec | urn/cap_urn_test.go:1489 |
| test578 | `Test578_rule1_duplicate_media_urns` | TEST578: RULE1 - duplicate media_urns rejected | cap/validation_test.go:134 |
| test579 | `Test579_rule2_empty_sources` | TEST579: RULE2 - empty sources rejected | cap/validation_test.go:145 |
| test580 | `Test580_rule3_different_stdin_urns` | TEST580: RULE3 - multiple stdin sources with different URNs rejected | cap/validation_test.go:155 |
| test581 | `Test581_rule3_same_stdin_urns_ok` | TEST581: RULE3 - multiple stdin sources with same URN is OK | cap/validation_test.go:169 |
| test582 | `Test582_rule4_duplicate_source_type` | TEST582: RULE4 - duplicate source type in single arg rejected | cap/validation_test.go:182 |
| test583 | `Test583_rule5_duplicate_position` | TEST583: RULE5 - duplicate position across args rejected | cap/validation_test.go:195 |
| test584 | `Test584_rule6_position_gap` | TEST584: RULE6 - position gap rejected (0, 2 without 1) | cap/validation_test.go:206 |
| test585 | `Test585_rule6_sequential_ok` | TEST585: RULE6 - sequential positions (0, 1, 2) pass | cap/validation_test.go:217 |
| test586 | `Test586_rule7_position_and_cli_flag` | TEST586: RULE7 - arg with both position and cli_flag rejected | cap/validation_test.go:227 |
| test587 | `Test587_rule9_duplicate_cli_flag` | TEST587: RULE9 - duplicate cli_flag across args rejected | cap/validation_test.go:240 |
| test588 | `Test588_rule10_reserved_cli_flags` | TEST588: RULE10 - reserved cli_flags rejected | cap/validation_test.go:251 |
| test589 | `Test589_all_rules_pass` | TEST589: valid cap args with mixed sources pass all rules | cap/validation_test.go:264 |
| test590 | `Test590_cli_flag_only_args` | TEST590: validate_cap_args accepts cap with only cli_flag sources (no positions) | cap/validation_test.go:282 |
| test591 | `Test591_is_more_specific_than` | TEST591: is_more_specific_than returns true when self has more tags for same request | cap/definition_test.go:256 |
| test592 | `Test592_remove_metadata` | TEST592: remove_metadata adds then removes metadata correctly | cap/definition_test.go:281 |
| test593 | `Test593_registered_by_lifecycle` | TEST593: registered_by lifecycle — set, get, clear | cap/definition_test.go:303 |
| test594 | `Test594_metadata_json_lifecycle` | TEST594: metadata_json lifecycle — set, get, clear | cap/definition_test.go:325 |
| test595 | `Test595_with_args_constructor` | TEST595: with_args constructor stores args correctly | cap/definition_test.go:344 |
| test596 | `Test596_with_full_definition_constructor` | TEST596: with_full_definition constructor stores all fields | cap/definition_test.go:363 |
| test597 | `Test597_cap_arg_with_full_definition` | TEST597: CapArg::with_full_definition stores all fields including optional ones | cap/definition_test.go:393 |
| test598 | `Test598_cap_output_lifecycle` | TEST598: CapOutput lifecycle — set_output, set/clear metadata | cap/definition_test.go:419 |
| test599 | `Test599_is_empty` | TEST599: is_empty returns true for empty response, false for non-empty | cap/response_test.go:286 |
| test600 | `Test600_size` | TEST600: size returns exact byte count for all content types | cap/response_test.go:301 |
| test601 | `Test601_get_content_type` | TEST601: get_content_type returns correct MIME type for each variant | cap/response_test.go:316 |
| test602 | `Test602_as_type_binary_error` | TEST602: as_type on binary response returns error (cannot deserialize binary) | cap/response_test.go:328 |
| test603 | `Test603_as_bool_edge_cases` | TEST603: as_bool handles all accepted truthy/falsy variants and rejects garbage | cap/response_test.go:337 |
| test605 | `Test605_all_coercion_paths_build_valid_urns` | TEST605: all_coercion_paths each entry builds a valid parseable CapUrn | standard/caps_test.go:81 |
| test606 | `Test606_coercion_urn_specs` | TEST606: coercion_urn in/out specs match the type's media URN constant | standard/caps_test.go:98 |
| test607 | `Test607_media_urns_for_extension_unknown` | TEST607: media_urns_for_extension returns error for unknown extension | media/spec_test.go:465 |
| test608 | `Test608_media_urns_for_extension_populated` | TEST608: media_urns_for_extension returns URNs after adding a spec with extensions | media/spec_test.go:475 |
| test609 | `Test609_get_extension_mappings` | TEST609: get_extension_mappings returns all registered extension→URN pairs. | media/spec_test.go:506 |
| test610 | `Test610_get_cached_spec` | TEST610: get_cached_spec returns None for unknown and Some for known | media/spec_test.go:533 |
| test612 | `Test612_clear_cache` | TEST612: clear_cache empties the in-memory cache for seeded schemas. | media/profile_test.go:85 |
| test613 | `Test613_validate_cached` | TEST613: validate_cached validates against cached standard schemas | media/profile_test.go:93 |
| test614 | `Test614_registry_creation` | TEST614: Verify registry creation succeeds and cache directory exists | cap/registry_test.go:26 |
| test616 | `Test616_stored_media_def_to_def` | TEST616: Verify StoredMediaDef converts to MediaDef preserving all fields | media/spec_test.go:565 |
| test617 | `Test617_normalize_media_urn` | TEST617: Verify normalize_media_urn produces consistent non-empty results | media/spec_test.go:584 |
| test618 | `Test618_registry_creation` | TEST618: Verify profile schema registry creation succeeds with temp cache | media/spec_test.go:553 |
| test619 | `Test619_fresh_registry_cache_is_empty` | TEST619: A freshly constructed registry has an empty cache. The well-known profile schemas are no longer bundled in the binary; callers must either fetch them on demand or seed via insert_schema. | media/profile_test.go:136 |
| test620 | `Test620_string_validation` | TEST620: Verify string schema validates strings and rejects non-strings | media/profile_test.go:146 |
| test621 | `Test621_integer_validation` | TEST621: Verify integer schema validates integers and rejects floats and strings | media/profile_test.go:153 |
| test622 | `Test622_number_validation` | TEST622: Verify number schema validates integers and floats, rejects strings | media/profile_test.go:161 |
| test623 | `Test623_boolean_validation` | TEST623: Verify boolean schema validates true/false and rejects string "true" | media/profile_test.go:169 |
| test624 | `Test624_object_validation` | TEST624: Verify object schema validates objects and rejects arrays | media/profile_test.go:177 |
| test625 | `Test625_string_array_validation` | TEST625: Verify string array schema validates string arrays and rejects mixed arrays | media/profile_test.go:184 |
| test626 | `Test626_unknown_profile_skips_validation` | TEST626: Verify unknown profile URL skips validation and returns Ok | media/profile_test.go:192 |
| test627 | `Test627_insert_schema_rejects_invalid_schema` | TEST627: insert_schema rejects malformed JSON Schemas instead of caching them. A registry that silently accepted invalid schemas would hide compilation problems until the first validation call. | media/profile_test.go:198 |
| test628 | `Test628_media_urn_constants_format` | TEST628: Verify media URN constants all start with "media:" prefix | urn/media_urn_test.go:666 |
| test629 | `Test629_profile_constants_format` | TEST629: Verify profile URL constants all start with capdag.com schema prefix | media/spec_test.go:670 |
| test630 | `Test630_cartridge_repo_creation` | TEST630: CartridgeRepo creation starts with empty cartridge list. | bifaci/cartridge_repo_test.go:752 |
| test631 | `Test631_needs_sync_empty_cache` | TEST631: needs_sync returns true with empty cache and non-empty URLs. | bifaci/cartridge_repo_test.go:760 |
| test632 | `Test632_deserialize_minimal_registry_cap` | TEST632: A registry cap with only the three required fields parses. | bifaci/cartridge_repo_test.go:921 |
| test633 | `Test633_deserialize_rich_registry_cap` | TEST633: A registry cap with cap_description, args, output all parses. | bifaci/cartridge_repo_test.go:948 |
| test634 | `Test634_deserialize_cap_group` | TEST634: A registry cap_group parses with caps + adapter_urns. | bifaci/cartridge_repo_test.go:1006 |
| test635 | `Test635_deserialize_cartridge_info_wire_shape` | TEST635: CartridgeInfo deserializes the wire shape exactly as returned by /api/cartridges (camelCase top-level + snake_case cap_groups). Null camelCase string fields fall back to empty. | bifaci/cartridge_repo_test.go:1030 |
| test636 | `Test636_deserialize_cartridge_info_with_null_strings` | TEST636: CartridgeInfo with null version/description/author still deserializes (the null_as_empty_string deserializer is the only tolerated coercion — every other malformed input is a hard error). | bifaci/cartridge_repo_test.go:1086 |
| test637 | `Test637_deserialize_full_registry_response` | TEST637: A full /api/cartridges-shaped response with two cartridges and nested cap_groups round-trips through the response wrapper. | bifaci/cartridge_repo_test.go:1117 |
| test638 | `Test638_no_peer_router_rejects_all` | TEST638: Verify NoPeerRouter rejects all requests with PeerInvokeNotSupported | bifaci/router_test.go:12 |
| test639 | `Test639_wildcard_001_empty_cap_is_illegal` | TEST639: bare/default top-to-top declared form is illegal | urn/cap_urn_test.go:1974 |
| test640 | `Test640_wildcard_002_in_only_is_illegal` | TEST640: cap:in defaults to the same illegal bare top form | urn/cap_urn_test.go:1979 |
| test641 | `Test641_wildcard_003_out_only_is_illegal` | TEST641: cap:out defaults to the same illegal bare top form | urn/cap_urn_test.go:1984 |
| test642 | `Test642_wildcard_004_in_out_no_values_become_media` | TEST642: cap:in;out becomes the same illegal bare top form | urn/cap_urn_test.go:1522 |
| test643 | `Test643_wildcard_005_explicit_asterisk_is_illegal` | TEST643: cap:in=*;out=* is the same illegal bare top form | urn/cap_urn_test.go:1989 |
| test644 | `Test644_wildcard_006_specific_in_wildcard_out_is_illegal` | TEST644: cap:in=media:;out=* is the same illegal bare top form | urn/cap_urn_test.go:1994 |
| test645 | `Test645_wildcard_007_wildcard_in_specific_out` | TEST645: cap:in=*;out=media:text has wildcard in, specific out | urn/cap_urn_test.go:1543 |
| test646 | `Test646_wildcard_008_invalid_in_spec_fails` | TEST646: cap:in=foo fails (invalid media URN) | urn/cap_urn_test.go:1551 |
| test647 | `Test647_wildcard_009_invalid_out_spec_fails` | TEST647: cap:in=media:;out=bar fails (invalid media URN) | urn/cap_urn_test.go:1557 |
| test648 | `Test648_wildcard_010_wildcard_accepts_specific` | TEST648: Wildcard in/out match specific caps | urn/cap_urn_test.go:1999 |
| test649 | `Test649_wildcard_011_specificity_scoring` | TEST649: Specificity - wildcard has 0, specific has tag count | urn/cap_urn_test.go:2010 |
| test650 | `Test650_wildcard_012_preserve_other_tags` | TEST650: cap:in=media:;out=media:;test preserves other tags | urn/cap_urn_test.go:1563 |
| test652 | `Test652_cap_identity_constant_works` | TEST652: CAP_IDENTITY constant matches identity caps regardless of string form | urn/cap_urn_test.go:1231 |
| test653 | `Test653_effect_none_illegal_declaration_rejected` | TEST653: invalid effect=none declarations fail at construction | urn/cap_urn_test.go:2021 |
| test667 | `Test667_verify_chunk_checksum_detects_corruption` | TEST667: verify_chunk_checksum detects corrupted payload | bifaci/frame_test.go:824 |
| test668 | `Test668_ResolveSlotWithPopulatedByteSlotValues` | TEST668: resolve_binding returns byte values when slot is populated with data | planner/argument_binding_test.go:20 |
| test669 | `Test669_ResolveSlotFallsBackToDefault` | TEST669: resolve_binding falls back to cap default value when slot has no data | planner/argument_binding_test.go:48 |
| test670 | `Test670_ResolveRequiredSlotNoValueReturnsErr` | TEST670: resolve_binding returns error when required slot has no value and no default | planner/argument_binding_test.go:69 |
| test671 | `Test671_ResolveOptionalSlotNoValueReturnsNone` | TEST671: resolve_binding returns None when optional slot has no value and no default | planner/argument_binding_test.go:83 |
| test675 | `Test675_build_request_frames_preserves_media_urn_in_stream_start` | TEST675: build_request_frames with full media URN preserves it in STREAM_START frame | bifaci/cartridge_runtime_test.go:3301 |
| test676 | `Test676_build_request_frames_round_trip_find_stream_succeeds` | TEST676: Full round-trip: build_request_frames → extract streams → find_stream succeeds | bifaci/cartridge_runtime_test.go:3325 |
| test677 | `Test677_base_urn_does_not_match_full_urn_in_find_stream` | TEST677: build_request_frames with BASE URN → find_stream with FULL URN FAILS This documents the root cause of the cartridge_client.rs bug: sender used "media:llm-generation-request" (base), receiver looked for "media:fmt=json;llm-generation-request;record" (full). is_equivalent requires exact tag set match, so base != full. | bifaci/cartridge_runtime_test.go:3352 |
| test678 | `Test678_find_stream_equivalent_urn` | TEST678: find_stream with exact equivalent URN (same tags, different order) succeeds | bifaci/cartridge_runtime_test.go:2939 |
| test679 | `Test679_find_stream_base_vs_full_fails` | TEST679: find_stream with base URN vs full URN fails — is_equivalent is strict This is the root cause of the cartridge_client.rs bug. Sender sent "media:llm-generation-request" but receiver looked for "media:fmt=json;llm-generation-request;record". | bifaci/cartridge_runtime_test.go:2956 |
| test680 | `Test680_require_stream_missing_fails` | TEST680: require_stream with missing URN returns hard StreamError | bifaci/cartridge_runtime_test.go:2967 |
| test681 | `Test681_find_stream_multiple` | TEST681: find_stream with multiple streams returns the correct one | bifaci/cartridge_runtime_test.go:2981 |
| test682 | `Test682_require_stream_returns_data` | TEST682: require_stream_str returns UTF-8 string for text data | bifaci/cartridge_runtime_test.go:2997 |
| test683 | `Test683_find_stream_invalid_urn_returns_nil` | TEST683: find_stream returns None for invalid media URN string (not a parse error — just None) | bifaci/cartridge_runtime_test.go:3011 |
| test688 | `Test688_is_multiple` | TEST688: Tests is_multiple method correctly identifies multi-value cardinalities Verifies Single returns false while Sequence and AtLeastOne return true | planner/cardinality_test.go:11 |
| test689 | `Test689_accepts_single` | TEST689: Tests accepts_single method identifies cardinalities that accept single values Verifies Single and AtLeastOne accept singles while Sequence does not | planner/cardinality_test.go:18 |
| test690 | `Test690_compatibility_single_to_single` | TEST690: Tests cardinality compatibility for single-to-single data flow Verifies Direct compatibility when both input and output are Single | planner/cardinality_test.go:25 |
| test691 | `Test691_compatibility_single_to_vector` | TEST691: Tests cardinality compatibility when wrapping single value into array Verifies WrapInArray compatibility when Sequence expects Single input | planner/cardinality_test.go:30 |
| test692 | `Test692_compatibility_vector_to_single` | TEST692: Tests cardinality compatibility when unwrapping array to singles Verifies RequiresFanOut compatibility when Single expects Sequence input | planner/cardinality_test.go:35 |
| test693 | `Test693_compatibility_vector_to_vector` | TEST693: Tests cardinality compatibility for sequence-to-sequence data flow Verifies Direct compatibility when both input and output are Sequence | planner/cardinality_test.go:40 |
| test697 | `Test697_cap_shape_info_one_to_one` | TEST697: Tests CapShapeInfo correctly identifies one-to-one pattern Verifies Single input and Single output result in OneToOne pattern | planner/cardinality_test.go:45 |
| test698 | `Test698_cap_shape_info_cardinality_always_single_from_urn` | TEST698: CapShapeInfo cardinality is always Single when derived from URN Cardinality comes from context (is_sequence), not from URN tags. The list tag is a semantic type property, not a cardinality indicator. | planner/cardinality_test.go:53 |
| test699 | `Test699_cap_shape_info_list_urn_still_single_cardinality` | TEST699: CapShapeInfo cardinality from URN is always Single; ManyToOne requires is_sequence | planner/cardinality_test.go:61 |
| test709 | `Test709_pattern_produces_vector` | TEST709: Tests CardinalityPattern correctly identifies patterns that produce vectors Verifies OneToMany and ManyToMany return true, others return false | planner/cardinality_test.go:80 |
| test710 | `Test710_pattern_requires_vector` | TEST710: Tests CardinalityPattern correctly identifies patterns that require vectors Verifies ManyToOne and ManyToMany return true, others return false | planner/cardinality_test.go:88 |
| test711 | `Test711_strand_shape_analysis_simple_linear` | TEST711: Tests shape chain analysis for simple linear one-to-one capability chains Verifies chains with no fan-out are valid and require no transformation | planner/cardinality_test.go:96 |
| test712 | `Test712_strand_shape_analysis_with_fan_out` | TEST712: Tests shape chain analysis detects fan-out points in capability chains Fan-out requires is_sequence=true on the cap's output, not a "list" URN tag | planner/cardinality_test.go:108 |
| test713 | `Test713_strand_shape_analysis_empty` | TEST713: Tests shape chain analysis handles empty capability chains correctly Verifies empty chains are valid and require no transformation | planner/cardinality_test.go:126 |
| test716 | `Test716_empty_collection` | TEST716: Tests CapInputCollection empty collection has zero files and folders Verifies is_empty() returns true and counts are zero for new collection | planner/collection_input_test.go:12 |
| test717 | `Test717_collection_with_files` | TEST717: Tests CapInputCollection correctly counts files in flat collection Verifies total_file_count() returns 2 for collection with 2 files, no folders | planner/collection_input_test.go:20 |
| test718 | `Test718_nested_collection` | TEST718: Tests CapInputCollection correctly counts files and folders in nested structure Verifies total_file_count() includes subfolder files and total_folder_count() counts subfolders | planner/collection_input_test.go:31 |
| test719 | `Test719_flatten_to_files` | TEST719: Tests CapInputCollection flatten_to_files recursively collects all files Verifies flatten() extracts files from root and all subfolders into flat list | planner/collection_input_test.go:46 |
| test720 | `Test720_from_media_urn_opaque` | TEST720: Tests InputStructure correctly identifies opaque media URNs Verifies that URNs without record marker are parsed as Opaque | planner/cardinality_test.go:148 |
| test721 | `Test721_from_media_urn_record` | TEST721: Tests InputStructure correctly identifies record media URNs Verifies that URNs with record marker tag are parsed as Record | planner/cardinality_test.go:157 |
| test722 | `Test722_structure_compatibility_opaque_to_opaque` | TEST722: Tests structure compatibility for opaque-to-opaque data flow | planner/cardinality_test.go:166 |
| test723 | `Test723_structure_compatibility_record_to_record` | TEST723: Tests structure compatibility for record-to-record data flow | planner/cardinality_test.go:172 |
| test724 | `Test724_structure_incompatibility_opaque_to_record` | TEST724: Tests structure incompatibility for opaque-to-record flow | planner/cardinality_test.go:178 |
| test725 | `Test725_structure_incompatibility_record_to_opaque` | TEST725: Tests structure incompatibility for record-to-opaque flow | planner/cardinality_test.go:184 |
| test726 | `Test726_apply_structure_add_record` | TEST726: Tests applying Record structure adds record marker to URN | planner/cardinality_test.go:190 |
| test727 | `Test727_apply_structure_remove_record` | TEST727: Tests applying Opaque structure removes record marker from URN | planner/cardinality_test.go:196 |
| test728 | `Test728_cap_node_helpers` | TEST728: Tests MachineNode helper methods for identifying node types (cap, fan-out, fan-in) Verifies is_cap(), is_fan_out(), is_fan_in(), and cap_urn() correctly classify node types | planner/plan_test.go:11 |
| test729 | `Test729_edge_types` | TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, JsonField) Verifies that edge constructors produce correct EdgeType variants | planner/plan_test.go:33 |
| test730 | `Test730_media_shape_from_urn_all_combinations` | TEST730: Tests MediaShape correctly parses all four combinations | planner/cardinality_test.go:202 |
| test731 | `Test731_media_shape_compatible_direct` | TEST731: Tests MediaShape compatibility for matching shapes | planner/cardinality_test.go:225 |
| test732 | `Test732_media_shape_cardinality_changes` | TEST732: Tests MediaShape compatibility for cardinality changes with matching structure | planner/cardinality_test.go:238 |
| test733 | `Test733_media_shape_structure_mismatch` | TEST733: Tests MediaShape incompatibility when structures don't match | planner/cardinality_test.go:254 |
| test734 | `Test734_topological_order_self_loop` | TEST734: Tests topological sort detects self-referencing cycles (A→A) Verifies that self-loops are recognized as cycles and produce an error | planner/plan_test.go:49 |
| test735 | `Test735_topological_order_multiple_entry_points` | TEST735: Tests topological sort handles graphs with multiple independent starting nodes Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering | planner/plan_test.go:60 |
| test736 | `Test736_topological_order_complex_dag` | TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes Verifies that all dependency constraints are satisfied in a graph with multiple converging paths | planner/plan_test.go:91 |
| test737 | `Test737_linear_chain_single_cap` | TEST737: Tests linear_chain() with exactly one capability Verifies that a single-element chain produces a valid plan with input_slot, cap, and output | planner/plan_test.go:134 |
| test738 | `Test738_linear_chain_empty` | TEST738: Tests linear_chain() with empty capability list Verifies that an empty chain produces a plan with zero nodes and edges | planner/plan_test.go:142 |
| test740 | `Test740_cap_shape_info_from_specs` | TEST740: Tests CapShapeInfo correctly parses cap specs | planner/cardinality_test.go:272 |
| test741 | `Test741_cap_shape_info_pattern` | TEST741: Tests CapShapeInfo pattern detection — OneToMany requires output is_sequence=true | planner/cardinality_test.go:281 |
| test747 | `Test747_cap_node_merge` | TEST747: Tests creation and validation of Merge node that combines multiple inputs Verifies that Merge nodes with multiple input nodes and a strategy can be added to plans | planner/plan_test.go:268 |
| test749 | `Test749_get_node` | TEST749: Tests get_node() method for looking up nodes by ID in a plan Verifies that existing nodes are found and non-existent nodes return None | planner/plan_test.go:318 |
| test750 | `Test750_strand_shape_valid` | TEST750: Tests shape chain analysis for valid chain with matching structures | planner/cardinality_test.go:292 |
| test751 | `Test751_strand_shape_structure_mismatch` | TEST751: Tests shape chain analysis detects structure mismatch | planner/cardinality_test.go:303 |
| test752 | `Test752_strand_shape_with_fanout` | TEST752: Tests shape chain analysis with fan-out (matching structures) Fan-out requires output is_sequence=true on the disbind cap | planner/cardinality_test.go:316 |
| test753 | `Test753_strand_shape_list_record_to_list_record` | TEST753: Tests shape chain analysis correctly handles list-to-list record flow | planner/cardinality_test.go:333 |
| test754 | `Test754_extract_prefix_nonexistent` | TEST754: extract_prefix_to with nonexistent node returns error | planner/plan_test.go:371 |
| test755 | `Test755_extract_foreach_body` | TEST755: extract_foreach_body extracts body as standalone plan | planner/plan_test.go:378 |
| test756 | `Test756_extract_foreach_body_unclosed` | TEST756: extract_foreach_body for unclosed ForEach (single body cap) | planner/plan_test.go:406 |
| test757 | `Test757_extract_foreach_body_wrong_type` | TEST757: extract_foreach_body fails for non-ForEach node | planner/plan_test.go:421 |
| test758 | `Test758_extract_suffix_from` | TEST758: extract_suffix_from extracts collect → cap_post → output | planner/plan_test.go:429 |
| test759 | `Test759_extract_suffix_nonexistent` | TEST759: extract_suffix_from fails for nonexistent node | planner/plan_test.go:446 |
| test760 | `Test760_decomposition_covers_all_caps` | TEST760: Full decomposition roundtrip — prefix + body + suffix cover all cap nodes | planner/plan_test.go:453 |
| test761 | `Test761_prefix_is_dag` | TEST761: Prefix sub-plan can be topologically sorted (is a valid DAG) | planner/plan_test.go:484 |
| test762 | `Test762_body_is_dag` | TEST762: Body sub-plan can be topologically sorted (is a valid DAG) | planner/plan_test.go:493 |
| test763 | `Test763_suffix_is_dag` | TEST763: Suffix sub-plan can be topologically sorted (is a valid DAG) | planner/plan_test.go:502 |
| test764 | `Test764_extract_prefix_to_input_slot` | TEST764: extract_prefix_to with InputSlot as target (trivial prefix) | planner/plan_test.go:740 |
| test765 | `Test765_validation_to_json_empty` | TEST765: Tests validation_to_json() returns None for empty validation constraints Verifies that default MediaValidation with no constraints produces JSON None | planner/plan_builder_test.go:166 |
| test766 | `Test766_validation_to_json_with_constraints` | TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON Verifies that min/max validation rules are correctly serialized as JSON fields | planner/plan_builder_test.go:173 |
| test770 | `Test770_rejects_foreach` | TEST770: plan_to_resolved_graph rejects plans containing ForEach nodes | orchestrator/orchestrator_test.go:155 |
| test772 | `Test772_find_paths_finds_multi_step_paths` | TEST772: Tests find_paths_to_exact_target() finds multi-step paths Verifies that paths through intermediate nodes are found correctly | planner/live_cap_fab_test.go:20 |
| test773 | `Test773_find_paths_returns_empty_when_no_path` | TEST773: Tests find_paths_to_exact_target() returns empty when no path exists Verifies that pathfinding returns no paths when target is unreachable | planner/live_cap_fab_test.go:43 |
| test774 | `Test774_get_reachable_targets_finds_all_targets` | TEST774: Tests get_reachable_targets() returns all reachable targets Verifies that reachable targets include direct cap targets and cardinality variants (list versions via Collect) | planner/live_cap_fab_test.go:60 |
| test777 | `Test777_type_mismatch_pdf_cap_does_not_match_png_input` | TEST777: Tests type checking prevents using PDF-specific cap with PNG input Verifies that media type compatibility is enforced during pathfinding | planner/live_cap_fab_test.go:93 |
| test778 | `Test778_type_mismatch_png_cap_does_not_match_pdf_input` | TEST778: Tests type checking prevents using PNG-specific cap with PDF input Verifies that media type compatibility is enforced during pathfinding | planner/live_cap_fab_test.go:108 |
| test779 | `Test779_get_reachable_targets_respects_type_matching` | TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps Verifies that PNG and PDF inputs reach different cap targets (not each other's) | planner/live_cap_fab_test.go:123 |
| test780 | `Test780_split_integer_array` | TEST780: split_cbor_array splits a simple array of integers | orchestrator/cbor_util_test.go:30 |
| test781 | `Test781_find_paths_respects_type_chain` | TEST781: Tests find_paths_to_exact_target() enforces type compatibility across multi-step chains Verifies that paths are only found when all intermediate types are compatible | planner/live_cap_fab_test.go:160 |
| test782 | `Test782_split_non_array` | TEST782: split_cbor_array rejects non-array input | orchestrator/cbor_util_test.go:46 |
| test783 | `Test783_split_empty_array` | TEST783: split_cbor_array rejects empty array | orchestrator/cbor_util_test.go:56 |
| test784 | `Test784_split_invalid_cbor` | TEST784: split_cbor_array rejects invalid CBOR bytes | orchestrator/cbor_util_test.go:66 |
| test785 | `Test785_assemble_integer_array` | TEST785: assemble_cbor_array creates array from individual items | orchestrator/cbor_util_test.go:75 |
| test786 | `Test786_roundtrip_split_assemble` | TEST786: split then assemble roundtrip preserves data | orchestrator/cbor_util_test.go:91 |
| test787 | `Test787_find_paths_sorting_prefers_shorter` | TEST787: Tests find_paths_to_exact_target() sorts paths by length, preferring shorter ones Verifies that among multiple paths, the shortest is ranked first | planner/live_cap_fab_test.go:187 |
| test788 | `Test788_foreach_only_with_sequence_input` | TEST788: ForEach is only synthesized when is_sequence=true | planner/live_cap_fab_test.go:210 |
| test789 | `Test789_cap_from_json_has_valid_specs` | TEST789: Tests that caps loaded from JSON have correct in_spec/out_spec | planner/live_cap_fab_test.go:594 |
| test790 | `Test790_identity_urn_is_specific` | TEST790: Tests identity_urn is specific and doesn't match everything | planner/live_cap_fab_test.go:617 |
| test792 | `Test792_ArgumentBindingRequiresInput` | TEST792: Tests ArgumentBinding requires_input distinguishes Slots from Literals Verifies Slot returns true (needs user input) while Literal returns false | planner/argument_binding_test.go:289 |
| test794 | `Test794_ArgumentBindingsAddFilePath` | TEST794: Tests ArgumentBindings add_file_path adds InputFilePath binding Verifies add_file_path() creates binding map entry with InputFilePath variant | planner/argument_binding_test.go:331 |
| test795 | `Test795_ArgumentBindingsUnresolvedSlots` | TEST795: ArgumentBindings identifies unresolved Slot bindings | planner/argument_binding_test.go:344 |
| test796 | `Test796_ResolveInputFilePath` | TEST796: Tests resolve_binding resolves InputFilePath to current file path Verifies InputFilePath binding resolves to file path bytes with InputFile source | planner/argument_binding_test.go:359 |
| test797 | `Test797_ResolveLiteral` | TEST797: Tests resolve_binding resolves Literal to JSON-encoded bytes Verifies Literal binding serializes value to bytes with Literal source | planner/argument_binding_test.go:382 |
| test798 | `Test798_ResolvePreviousOutput` | TEST798: Tests resolve_binding extracts value from previous node output Verifies PreviousOutput binding fetches field from earlier execution results | planner/argument_binding_test.go:401 |
| test804 | `Test804_ExtractJsonPathSimple` | TEST804: Tests basic JSON path extraction with dot notation for nested objects Verifies that simple paths like "data.message" correctly extract values from nested JSON structures | planner/executor_test.go:8 |
| test805 | `Test805_ExtractJsonPathWithArray` | TEST805: Tests JSON path extraction with array indexing syntax | planner/executor_test.go:24 |
| test806 | `Test806_ExtractJsonPathMissingField` | TEST806: Tests error handling when JSON path references non-existent fields Verifies that accessing missing fields returns an appropriate error message | planner/executor_test.go:41 |
| test807 | `Test807_ApplyEdgeTypeDirect` | TEST807: Tests EdgeType::Direct passes JSON values through unchanged Verifies that Direct edge type acts as a transparent passthrough without transformation | planner/executor_test.go:54 |
| test808 | `Test808_ApplyEdgeTypeJsonField` | TEST808: Tests EdgeType::JsonField extracts specific top-level fields from JSON objects Verifies that JsonField edge type correctly isolates a single named field from the source output | planner/executor_test.go:67 |
| test809 | `Test809_ApplyEdgeTypeJsonFieldMissing` | TEST809: Tests EdgeType::JsonField error handling for missing fields Verifies that attempting to extract a non-existent field returns an error | planner/executor_test.go:79 |
| test810 | `Test810_ApplyEdgeTypeJsonPath` | TEST810: Tests EdgeType::JsonPath extracts values using nested path expressions Verifies that JsonPath edge type correctly navigates through multiple levels like "data.nested.value" | planner/executor_test.go:88 |
| test811 | `Test811_ApplyEdgeTypeIteration` | TEST811: Tests EdgeType::Iteration preserves array values for iterative processing Verifies that Iteration edge type passes through arrays unchanged to enable ForEach patterns | planner/executor_test.go:106 |
| test812 | `Test812_ApplyEdgeTypeCollection` | TEST812: Tests EdgeType::Collection preserves collected values without transformation Verifies that Collection edge type maintains structure for aggregation patterns | planner/executor_test.go:119 |
| test813 | `Test813_ExtractJsonPathDeeplyNested` | TEST813: Tests JSON path extraction through deeply nested object hierarchies (4+ levels) Verifies that paths can traverse multiple nested levels like "level1.level2.level3.level4.value" | planner/executor_test.go:133 |
| test814 | `Test814_ExtractJsonPathArrayOutOfBounds` | TEST814: Tests error handling when array index exceeds available elements Verifies that out-of-bounds array access returns a descriptive error message | planner/executor_test.go:155 |
| test815 | `Test815_ExtractJsonPathSingleSegment` | TEST815: Tests JSON path extraction with single-level paths (no nesting) Verifies that simple field names without dots correctly extract top-level values | planner/executor_test.go:169 |
| test816 | `Test816_ExtractJsonPathWithSpecialCharacters` | TEST816: Tests JSON path extraction preserves special characters in string values Verifies that quotes, backslashes, and other special characters are correctly maintained | planner/executor_test.go:181 |
| test817 | `Test817_ExtractJsonPathWithNullValue` | TEST817: Tests JSON path extraction correctly handles explicit null values Verifies that null is returned as serde_json::Value::Null rather than an error | planner/executor_test.go:198 |
| test818 | `Test818_ExtractJsonPathWithEmptyArray` | TEST818: Tests JSON path extraction correctly returns empty arrays Verifies that zero-length arrays are extracted as valid empty array values | planner/executor_test.go:214 |
| test819 | `Test819_ExtractJsonPathWithNumericTypes` | TEST819: Tests JSON path extraction handles various numeric types correctly Verifies extraction of integers, floats, negative numbers, and zero | planner/executor_test.go:231 |
| test820 | `Test820_ExtractJsonPathWithBoolean` | TEST820: Tests JSON path extraction correctly handles boolean values Verifies that true and false are extracted as proper boolean JSON values | planner/executor_test.go:259 |
| test821 | `Test821_ExtractJsonPathWithNestedArrays` | TEST821: Tests JSON path extraction with multi-dimensional arrays (matrix access) Verifies that nested array structures like "matrix[1]" correctly extract inner arrays | planner/executor_test.go:283 |
| test822 | `Test822_ExtractJsonPathInvalidArrayIndex` | TEST822: Tests error handling for non-numeric array indices Verifies that invalid indices like "items[abc]" return a descriptive parse error | planner/executor_test.go:301 |
| test823 | `Test823_dispatch_exact_match` | TEST823: is_dispatchable — exact match provider dispatches request | urn/cap_urn_test.go:1264 |
| test824 | `Test824_dispatch_contravariant_input` | TEST824: is_dispatchable — provider with broader input handles specific request (contravariance) | urn/cap_urn_test.go:1273 |
| test825 | `Test825_dispatch_request_unconstrained_input` | TEST825: is_dispatchable — request with unconstrained input dispatches to specific provider media: on the request input axis means "unconstrained" — vacuously true | urn/cap_urn_test.go:1282 |
| test826 | `Test826_dispatch_covariant_output` | TEST826: is_dispatchable — provider output must satisfy request output (covariance) | urn/cap_urn_test.go:1292 |
| test827 | `Test827_dispatch_generic_output_fails` | TEST827: is_dispatchable — provider with generic output cannot satisfy specific request | urn/cap_urn_test.go:1302 |
| test828 | `Test828_dispatch_wildcard_requires_tag_presence` | TEST828: is_dispatchable — wildcard * tag in request, provider missing tag → reject | urn/cap_urn_test.go:1312 |
| test829 | `Test829_dispatch_wildcard_with_tag_present` | TEST829: is_dispatchable — wildcard * tag in request, provider has tag → accept | urn/cap_urn_test.go:1322 |
| test830 | `Test830_dispatch_provider_extra_tags` | TEST830: is_dispatchable — provider extra tags are refinement, always OK | urn/cap_urn_test.go:1332 |
| test831 | `Test831_dispatch_cross_backend_mismatch` | TEST831: is_dispatchable — cross-backend mismatch prevented | urn/cap_urn_test.go:1342 |
| test832 | `Test832_dispatch_asymmetric` | TEST832: is_dispatchable is NOT symmetric | urn/cap_urn_test.go:1352 |
| test833 | `Test833_comparable_symmetric` | TEST833: is_comparable — both directions checked | urn/cap_urn_test.go:1362 |
| test834 | `Test834_comparable_unrelated` | TEST834: is_comparable — unrelated caps are NOT comparable | urn/cap_urn_test.go:1372 |
| test835 | `Test835_equivalent_identical` | TEST835: is_equivalent — identical caps | urn/cap_urn_test.go:1382 |
| test836 | `Test836_equivalent_non_equivalent` | TEST836: is_equivalent — non-equivalent comparable caps | urn/cap_urn_test.go:1392 |
| test837 | `Test837_dispatch_op_mismatch` | TEST837: is_dispatchable — op tag mismatch rejects | urn/cap_urn_test.go:1402 |
| test838 | `Test838_dispatch_request_wildcard_output` | TEST838: is_dispatchable — request with wildcard output accepts any provider output | urn/cap_urn_test.go:1411 |
| test839 | `Test839_peer_response_delivers_logs_before_stream_start` | TEST839: LOG frames arriving BEFORE StreamStart are delivered immediately This tests the critical fix: during a peer call, the peer (e.g., modelcartridge) sends LOG frames for minutes during model download BEFORE sending any data (StreamStart + Chunk). The handler must receive these LOGs in real-time so it can re-emit progress and keep the engine's activity timer alive. Previously, demux_single_stream blocked on awaiting StreamStart before returning PeerResponse, which meant the handler couldn't call recv() until data arrived — causing 120s activity timeouts during long downloads. | bifaci/cartridge_runtime_test.go:2792 |
| test840 | `Test840_peer_response_collect_bytes_discards_logs` | TEST840: PeerResponse::collect_bytes discards LOG frames | bifaci/cartridge_runtime_test.go:2860 |
| test841 | `Test841_peer_response_collect_value_discards_logs` | TEST841: PeerResponse::collect_value discards LOG frames | bifaci/cartridge_runtime_test.go:2888 |
| test842 | `Test842_progress_sender_emits_frames` | TEST842: run_with_keepalive returns closure result (fast operation, no keepalive frames) | bifaci/cartridge_runtime_test.go:3036 |
| test843 | `Test843_progress_sender_from_goroutine` | TEST843: run_with_keepalive returns Ok/Err from closure | bifaci/cartridge_runtime_test.go:3082 |
| test844 | `Test844_progress_sender_multiple_goroutines` | TEST844: run_with_keepalive propagates errors from closure | bifaci/cartridge_runtime_test.go:3115 |
| test845 | `Test845_progress_sender_independent_of_emitter` | TEST845: ProgressSender emits progress and log frames independently of OutputStream | bifaci/cartridge_runtime_test.go:3160 |
| test846 | `Test846_progress_frame_roundtrip` | TEST846: Test progress LOG frame encode/decode roundtrip preserves progress float | bifaci/io_test.go:1056 |
| test847 | `Test847_progress_double_roundtrip` | TEST847: Double roundtrip (modelcartridge → relay → candlecartridge) | bifaci/io_test.go:1109 |
| test848 | `Test848_relay_notify_roundtrip` | TEST848: RelayNotify encode/decode roundtrip preserves manifest and limits | bifaci/io_test.go:887 |
| test849 | `Test849_relay_state_roundtrip` | TEST849: RelayState encode/decode roundtrip preserves resource payload | bifaci/io_test.go:928 |
| test850 | `Test850_all_format_conversion_paths_build_valid_urns` | TEST850: all_format_conversion_paths each entry builds a valid parseable CapUrn | standard/caps_test.go:111 |
| test851 | `Test851_format_conversion_urn_specs` | TEST851: format_conversion_urn in/out specs match the input constants | standard/caps_test.go:125 |
| test852 | `Test852_lub_identical` | TEST852: LUB of identical URNs returns the same URN | urn/media_urn_test.go:580 |
| test853 | `Test853_lub_no_common_tags` | TEST853: LUB of URNs with no common tags returns media: (universal) | urn/media_urn_test.go:588 |
| test854 | `Test854_lub_partial_overlap` | TEST854: LUB keeps common tags, drops differing ones | urn/media_urn_test.go:600 |
| test855 | `Test855_lub_list_vs_scalar` | TEST855: LUB of list and non-list drops list tag | urn/media_urn_test.go:612 |
| test856 | `Test856_lub_empty` | TEST856: LUB of empty input returns universal type | urn/media_urn_test.go:624 |
| test857 | `Test857_lub_single` | TEST857: LUB of single input returns that input | urn/media_urn_test.go:632 |
| test858 | `Test858_lub_three_inputs` | TEST858: LUB with three+ inputs narrows correctly | urn/media_urn_test.go:640 |
| test859 | `Test859_lub_valued_tags` | TEST859: LUB with valued tags (non-marker) that differ | urn/media_urn_test.go:654 |
| test860 | `Test860_seq_assigner_same_rid_different_xids_independent` | TEST860: Same RID with different XIDs get independent seq counters | bifaci/frame_test.go:1008 |
| test880 | `Test880_no_duplicates_with_unique_caps` | TEST880: Tests duplicate detection passes for caps with unique URN combinations Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations | planner/plan_builder_test.go:192 |
| test886 | `Test886_optional_non_io_arg_with_default_has_default` | TEST886: Tests optional non-IO arguments with default values are marked as HasDefault Verifies that optional arguments with defaults behave the same as required ones with defaults | planner/plan_builder_test.go:185 |
| test890 | `Test890_direction_semantic_matching` | TEST890: Semantic direction matching - generic provider matches specific request | urn/cap_urn_test.go:939 |
| test891 | `Test891_direction_semantic_specificity` | TEST891: Semantic direction specificity — more constraints in either axis means a higher score under the truth-table-driven sum. media: (top, no tags) scores 0; each marker tag scores 2; each exact tag scores 3. | urn/cap_urn_test.go:996 |
| test892 | `Test892_extensions_serialization` | TEST892: Test extensions serializes/deserializes correctly in MediaDef | media/spec_test.go:378 |
| test893 | `Test893_extensions_with_metadata_and_validation` | TEST893: Test extensions can coexist with metadata and validation | media/spec_test.go:403 |
| test894 | `Test894_multiple_extensions` | TEST894: Test multiple extensions in a media def | media/spec_test.go:437 |
| test920 | `Test920_single_cap_plan` | TEST920: Tests creation of a simple execution plan with a single capability Verifies that single_cap() generates a valid plan with input_slot, cap node, and output node | planner/plan_test.go:511 |
| test921 | `Test921_linear_chain_plan` | TEST921: LinearChain creates a plan with correct nodes and edges in topological order. | planner/plan_test.go:521 |
| test922 | `Test922_empty_plan` | TEST922: An empty MachinePlan is valid with zero nodes. | planner/plan_test.go:540 |
| test923 | `Test923_plan_with_metadata` | TEST923: MachinePlan stores and retrieves metadata by key. | planner/plan_test.go:547 |
| test924 | `Test924_validate_invalid_edge` | TEST924: Tests plan validation detects edges pointing to non-existent nodes Verifies that validate() returns an error when an edge references a missing to_node | planner/plan_test.go:559 |
| test925 | `Test925_topological_order_diamond` | TEST925: Tests topological sort correctly orders a diamond-shaped DAG (A->B,C->D) Verifies that nodes with multiple paths respect dependency constraints (A first, D last) | planner/plan_test.go:570 |
| test926 | `Test926_topological_order_detects_cycle` | TEST926: Tests topological sort detects and rejects cyclic dependencies (A->B->C->A) Verifies that circular references produce a "Cycle detected" error | planner/plan_test.go:592 |
| test927 | `Test927_execution_result` | TEST927: Tests MachineResult structure for successful execution outcomes Verifies that success status, outputs, and primary_output() accessor work correctly | planner/plan_test.go:610 |
| test928 | `Test928_validate_invalid_from_node` | TEST928: Tests plan validation detects edges originating from non-existent nodes Verifies that validate() returns an error when an edge references a missing from_node | planner/plan_test.go:625 |
| test929 | `Test929_validate_invalid_entry_node` | TEST929: Tests plan validation detects invalid entry node references Verifies that validate() returns an error when entry_nodes contains a non-existent node ID | planner/plan_test.go:636 |
| test930 | `Test930_validate_invalid_output_node` | TEST930: Tests plan validation detects invalid output node references Verifies that validate() returns an error when output_nodes contains a non-existent node ID | planner/plan_test.go:647 |
| test931 | `Test931_node_execution_result_failure` | TEST931: Tests NodeExecutionResult structure for failed node execution Verifies that failure status, error message, and absence of outputs are correctly represented | planner/plan_test.go:658 |
| test932 | `Test932_execution_result_failure` | TEST932: Tests MachineResult structure for failed chain execution Verifies that failure status, error message, and absence of outputs are correctly represented | planner/plan_test.go:673 |
| test933 | `Test933_serialization_roundtrip` | TEST933: Tests CapInputCollection serializes to JSON and deserializes correctly Verifies JSON round-trip preserves folder_id, folder_name, files and file metadata | planner/collection_input_test.go:64 |
| test934 | `Test934_find_first_foreach` | TEST934: find_first_foreach detects ForEach in a plan | planner/plan_test.go:688 |
| test935 | `Test935_find_first_foreach_linear` | TEST935: find_first_foreach returns None for linear plans | planner/plan_test.go:696 |
| test936 | `Test936_has_foreach` | TEST936: has_foreach detects ForEach nodes | planner/plan_test.go:702 |
| test937 | `Test937_extract_prefix_to` | TEST937: extract_prefix_to extracts input_slot -> cap_0 as a standalone plan | planner/plan_test.go:719 |
| test939 | `Test939_cap_urn_canonical_form_drops_wildcard_in_out` | TEST939: The canonical form drops `in=media:` and `out=media:` segments. Every spelling of "the same cap with wildcard in/out" collapses to one byte-identical canonical string. This is the contract that makes registry lookups work: the cap-publisher hashes `<canonical-urn>` to compute the cache key, and every language port (Rust, Go, Python, JS, ObjC) must agree on the canonical form for cross-language lookups to land on the same key. A regression that emitted the wildcard segments would silently move the published cap to a different SHA-256 bucket, 404'ing every reader that hashes the canonical form. | urn/cap_urn_test.go:330 |
| test953 | `Test953_linear_plan_still_works` | TEST953: Linear plans (no ForEach/Collect) still convert successfully | orchestrator/orchestrator_test.go:186 |
| test954 | `Test954_standalone_collect_passthrough` | TEST954: Standalone Collect nodes are handled as pass-through Plan: input → cap_0 → Collect → cap_1 → output The standalone Collect is transparent — the resolved edge from Collect to cap_1 should be rewritten to go from cap_0 to cap_1 directly. | orchestrator/orchestrator_test.go:206 |
| test955 | `Test955_split_map_array` | TEST955: split_cbor_array with nested maps | orchestrator/cbor_util_test.go:110 |
| test956 | `Test956_roundtrip_assemble_split` | TEST956: assemble then split roundtrip preserves data | orchestrator/cbor_util_test.go:127 |
| test957 | `Test957_cap_input_file_new` | TEST957: Tests CapInputFile constructor creates file with correct path and media URN Verifies new() initializes file_path, media_urn and leaves metadata/source_id as None | planner/argument_binding_test.go:501 |
| test958 | `Test958_cap_input_file_from_listing` | TEST958: Tests CapInputFile from_listing sets source metadata correctly Verifies from_listing() populates source_id and source_type as Listing | planner/argument_binding_test.go:518 |
| test959 | `Test959_cap_input_file_filename` | TEST959: CapInputFile.Filename() extracts the basename from a full path. | planner/argument_binding_test.go:529 |
| test960 | `Test960_argument_binding_literal_string` | TEST960: NewLiteralStringBinding creates a Literal binding wrapping a JSON string. | planner/argument_binding_test.go:541 |
| test961 | `Test961_assemble_empty` | TEST961: assemble empty list produces empty CBOR array | orchestrator/cbor_util_test.go:144 |
| test962 | `Test962_assemble_invalid_item` | TEST962: assemble rejects invalid CBOR item | orchestrator/cbor_util_test.go:154 |
| test963 | `Test963_split_binary_items` | TEST963: split preserves CBOR byte strings (binary data — the common case in bifaci) | orchestrator/cbor_util_test.go:167 |
| test964 | `Test964_split_sequence_bytes` | TEST964: split_cbor_sequence splits concatenated CBOR Bytes values | orchestrator/cbor_util_test.go:185 |
| test965 | `Test965_split_sequence_text` | TEST965: split_cbor_sequence splits concatenated CBOR Text values | orchestrator/cbor_util_test.go:206 |
| test966 | `Test966_split_sequence_mixed` | TEST966: split_cbor_sequence handles mixed types | orchestrator/cbor_util_test.go:221 |
| test967 | `Test967_split_sequence_single` | TEST967: split_cbor_sequence single-item sequence | orchestrator/cbor_util_test.go:243 |
| test968 | `Test968_roundtrip_assemble_split_sequence` | TEST968: roundtrip — assemble then split preserves items | orchestrator/cbor_util_test.go:256 |
| test969 | `Test969_roundtrip_split_assemble_sequence` | TEST969: roundtrip — split then assemble preserves byte-for-byte | orchestrator/cbor_util_test.go:275 |
| test970 | `Test970_split_sequence_empty` | TEST970: split_cbor_sequence rejects empty data | orchestrator/cbor_util_test.go:290 |
| test971 | `Test971_split_sequence_truncated` | TEST971: split_cbor_sequence rejects truncated CBOR | orchestrator/cbor_util_test.go:299 |
| test972 | `Test972_assemble_sequence_invalid_item` | TEST972: assemble_cbor_sequence rejects invalid CBOR item | orchestrator/cbor_util_test.go:314 |
| test973 | `Test973_assemble_sequence_empty` | TEST973: assemble_cbor_sequence with empty items list produces empty bytes | orchestrator/cbor_util_test.go:327 |
| test974 | `Test974_sequence_is_not_array` | TEST974: CBOR sequence is NOT a CBOR array — split_cbor_array rejects a sequence | orchestrator/cbor_util_test.go:334 |
| test975 | `Test975_single_value_sequence` | TEST975: split_cbor_sequence works on data that is also a valid CBOR array (single top-level value) | orchestrator/cbor_util_test.go:353 |
| test977 | `Test977_OsFilesExcludedIntegration` | TEST977: OS files excluded in resolve_paths | input_resolver/resolver_test.go:164 |
| test991 | `Test991_detects_duplicate_cap_urns` | TEST991: Tests duplicate detection identifies caps with identical URNs Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn | planner/plan_builder_test.go:206 |
| test992 | `Test992_different_ops_same_types_not_duplicates` | TEST992: Tests caps with different operations but same input/output types are not duplicates Verifies that only the complete URN (including op) is used for duplicate detection | planner/plan_builder_test.go:220 |
| test993 | `Test993_same_op_different_input_types_not_duplicates` | TEST993: Tests caps with same operation but different input types are not duplicates Verifies that input type differences distinguish caps with the same operation name | planner/plan_builder_test.go:232 |
| test994 | `Test994_input_arg_first_cap_auto_resolved_from_input` | TEST994: Tests first cap's input argument is automatically resolved from input file Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain | planner/plan_builder_test.go:244 |
| test995 | `Test995_input_arg_subsequent_cap_auto_resolved_from_previous` | TEST995: Tests subsequent caps' input arguments are automatically resolved from previous output Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first | planner/plan_builder_test.go:250 |
| test996 | `Test996_output_arg_auto_resolved` | TEST996: Tests output arguments are automatically resolved from previous cap's output Verifies that arguments matching the output spec are always resolved as FromPreviousOutput | planner/plan_builder_test.go:259 |
| test997 | `Test997_file_path_type_fallback_first_cap` | TEST997: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap Verifies that generic file-path arguments are bound to input file in the first cap | planner/plan_builder_test.go:265 |
| test998 | `Test998_file_path_type_fallback_subsequent_cap` | TEST998: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps Verifies that generic file-path arguments are bound to previous cap's output after the first cap | planner/plan_builder_test.go:271 |
| test1000 | `Test1000_SingleExistingFile` | TEST1000: Single existing file | input_resolver/path_resolver_test.go:29 |
| test1001 | `Test1001_NonexistentFile` | TEST1001: Single non-existent file | input_resolver/path_resolver_test.go:47 |
| test1002 | `Test1002_EmptyDirectory` | TEST1002: Empty directory | input_resolver/path_resolver_test.go:56 |
| test1003 | `Test1003_DirectoryWithFiles` | TEST1003: Directory with files | input_resolver/path_resolver_test.go:68 |
| test1004 | `Test1004_DirectoryWithSubdirs` | TEST1004: Directory with subdirs (recursive) | input_resolver/path_resolver_test.go:84 |
| test1005 | `Test1005_GlobMatchingFiles` | TEST1005: Glob matching files | input_resolver/path_resolver_test.go:102 |
| test1006 | `Test1006_GlobMatchingNothing` | TEST1006: Glob matching nothing | input_resolver/path_resolver_test.go:119 |
| test1007 | `Test1007_RecursiveGlob` | TEST1007: Recursive glob | input_resolver/path_resolver_test.go:134 |
| test1008 | `Test1008_MixedFileDir` | TEST1008: Mixed file + dir | input_resolver/path_resolver_test.go:153 |
| test1009 | `Test1009_non_io_arg_with_default_has_default` | TEST1009: Tests required non-IO arguments with default values are marked as HasDefault Verifies that arguments like integers with defaults don't require user input | planner/plan_builder_test.go:277 |
| test1010 | `Test1010_DuplicatePaths` | TEST1010: Duplicate paths are deduplicated | input_resolver/path_resolver_test.go:177 |
| test1011 | `Test1011_InvalidGlob` | TEST1011: Invalid glob syntax | input_resolver/path_resolver_test.go:197 |
| test1012 | `Test1012_non_io_arg_without_default_requires_user_input` | TEST1012: Tests required non-IO arguments without defaults require user input Verifies that arguments like strings without defaults are marked as RequiresUserInput | planner/plan_builder_test.go:284 |
| test1013 | `Test1013_EmptyInput` | TEST1013: Empty input array | input_resolver/path_resolver_test.go:206 |
| test1014 | `Test1014_SymlinkToFile` | TEST1014: Symlink to file | input_resolver/path_resolver_test.go:215 |
| test1015 | `Test1015_optional_non_io_arg_without_default_requires_user_input` | TEST1015: Tests optional non-IO arguments without defaults still require user input Verifies that optional arguments without defaults must be explicitly provided or skipped | planner/plan_builder_test.go:290 |
| test1016 | `Test1016_PathWithSpaces` | TEST1016: Path with spaces | input_resolver/path_resolver_test.go:237 |
| test1017 | `Test1017_PathWithUnicode` | TEST1017: Path with unicode | input_resolver/path_resolver_test.go:252 |
| test1018 | `Test1018_RelativePath` | TEST1018: Relative path | input_resolver/path_resolver_test.go:267 |
| test1019 | `Test1019_validation_to_json_nil` | TEST1019: Tests validation_to_json() returns None for None input Verifies that missing validation metadata is converted to JSON None | planner/plan_builder_test.go:296 |
| test1020 | `Test1020_ds_store_excluded` | TEST1020: macOS .DS_Store is excluded | input_resolver/os_filter_test.go:10 |
| test1021 | `Test1021_thumbs_db_excluded` | TEST1021: Windows Thumbs.db is excluded | input_resolver/os_filter_test.go:16 |
| test1022 | `Test1022_resource_fork_excluded` | TEST1022: macOS resource fork files are excluded | input_resolver/os_filter_test.go:22 |
| test1023 | `Test1023_office_lock_excluded` | TEST1023: Office lock files are excluded | input_resolver/os_filter_test.go:28 |
| test1024 | `Test1024_git_dir_excluded` | TEST1024: .git directory is excluded | input_resolver/os_filter_test.go:34 |
| test1025 | `Test1025_macosx_dir_excluded` | TEST1025: __MACOSX archive artifact is excluded | input_resolver/os_filter_test.go:40 |
| test1026 | `Test1026_temp_files_excluded` | TEST1026: Temp files are excluded | input_resolver/os_filter_test.go:46 |
| test1027 | `Test1027_localized_excluded` | TEST1027: .localized is excluded | input_resolver/os_filter_test.go:54 |
| test1028 | `Test1028_desktop_ini_excluded` | TEST1028: desktop.ini is excluded | input_resolver/os_filter_test.go:59 |
| test1029 | `Test1029_normal_files_not_excluded` | TEST1029: Normal files are NOT excluded | input_resolver/os_filter_test.go:64 |
| test1090 | `Test1090_SingleFileScalar` | TEST1090: 1 file → is_sequence=false | input_resolver/resolver_test.go:89 |
| test1092 | `Test1092_TwoFiles` | TEST1092: 2 files → is_sequence=true | input_resolver/resolver_test.go:107 |
| test1093 | `Test1093_DirSingleFile` | TEST1093: 1 dir with 1 file → is_sequence=false | input_resolver/resolver_test.go:126 |
| test1094 | `Test1094_DirMultipleFiles` | TEST1094: 1 dir with 3 files → is_sequence=true | input_resolver/resolver_test.go:144 |
| test1098 | `Test1098_ExtensionBasedPdf` | TEST1098: Extension-based detection picks up pdf tag for .pdf files | input_resolver/resolver_test.go:183 |
| test1100 | `Test1100_cap_urn_normalizes_media_urn_tag_order` | TEST1100: Tests that CapUrn normalizes media URN tags to canonical order Two CapUrns with different tag ordering in out spec must produce the same canonical string. | planner/plan_builder_test.go:303 |
| test1103 | `Test1103_is_dispatchable_uses_correct_directionality` | TEST1103: Tests that IsDispatchable has correct directionality A specific provider is dispatchable for a general request; the reverse is false. | planner/plan_builder_test.go:320 |
| test1104 | `Test1104_is_dispatchable_rejects_non_dispatchable` | TEST1104: Tests that is_dispatchable rejects when provider cannot dispatch request | planner/plan_builder_test.go:334 |
| test1105 | `Test1105_TwoStepsSameCapUrnDifferentSlotValues` | TEST1105: Two steps with the same cap_urn get distinct slot values via different node_ids. This is the core disambiguation scenario that step-index keying was designed to solve. | planner/argument_binding_test.go:96 |
| test1106 | `Test1106_SlotFallsThroughToCapSettingsShared` | TEST1106: Slot resolution falls through to cap_settings when no slot_value exists. cap_settings are keyed by cap_urn (shared across steps), so both steps get the same value. | planner/argument_binding_test.go:138 |
| test1107 | `Test1107_SlotValueOverridesCapSettingsPerStep` | TEST1107: step_0 has a slot_value override, step_1 falls through to cap_settings. Proves per-step override works while shared settings remain as fallback. | planner/argument_binding_test.go:174 |
| test1108 | `Test1108_ResolveAllPassesNodeID` | TEST1108: ResolveAll with node_id threads correctly through to each binding. | planner/argument_binding_test.go:216 |
| test1109 | `Test1109_SlotKeyUsesNodeIDNotCapUrn` | TEST1109: Slot key uses node_id, NOT cap_urn — a slot_value keyed by cap_urn must not match. | planner/argument_binding_test.go:267 |
| test1111 | `Test1111_foreach_for_user_provided_list_source` | TEST1111: ForEach works for user-provided list sources not in the graph. User provides media:list;txt;enc=utf-8 with is_sequence=true → ForEach+cap path found. | planner/live_cap_fab_test.go:247 |
| test1112 | `Test1112_no_collect_in_path_finding` | TEST1112: Collect is not synthesized during path finding. Reaching a list target type requires the cap itself to output a list type. | planner/live_cap_fab_test.go:287 |
| test1113 | `Test1113_multi_cap_path_no_collect` | TEST1113: Multi-cap path without Collect — Collect is not synthesized | planner/live_cap_fab_test.go:310 |
| test1114 | `Test1114_graph_stores_only_cap_edges` | TEST1114: Graph stores only Cap edges after sync | planner/live_cap_fab_test.go:333 |
| test1115 | `Test1115_dynamic_foreach_with_is_sequence` | TEST1115: ForEach is synthesized when is_sequence=true AND caps can consume items | planner/live_cap_fab_test.go:351 |
| test1116 | `Test1116_collect_never_synthesized` | TEST1116: Collect is never synthesized during path finding | planner/live_cap_fab_test.go:384 |
| test1117 | `Test1117_no_foreach_when_not_sequence` | TEST1117: ForEach is NOT synthesized when is_sequence=false | planner/live_cap_fab_test.go:403 |
| test1118 | `Test1118_no_foreach_without_cap_consumers` | TEST1118: ForEach not synthesized without cap consumers even with is_sequence=true | planner/live_cap_fab_test.go:424 |
| test1119 | `Test1119_FromStrand_returns_single_strand_machine` | TEST1119: Strand::knit returns a single-strand Machine via the new resolver. Smoke test the registry-threaded API end-to-end. | machine/machine_test.go:656 |
| test1120 | `Test1120_FromStrand_unknown_cap_fails_hard` | TEST1120: Strand::knit fails hard when the cap is not in the registry — the planner produces strands referencing caps that must be present in the cap registry's cache for resolution to succeed. | machine/machine_test.go:682 |
| test1127 | `Test1127_cap_documentation_round_trip_with_markdown_body` | TEST1127: Documentation field round-trips through JSON serialize/deserialize. The body must survive multi-line markdown with CRLF, backticks, double quotes, and Unicode characters — every character must be preserved. | cap/definition_test.go:535 |
| test1128 | `Test1128_cap_documentation_omitted_when_none` | TEST1128: When Documentation is nil, the serializer must omit the field entirely. There must be no "documentation":null — only absence. | cap/definition_test.go:557 |
| test1129 | `Test1129_cap_documentation_parses_from_capfab_json` | TEST1129: A capfab-shaped JSON document with a documentation field must deserialize into a Cap with the body intact. | cap/definition_test.go:575 |
| test1130 | `Test1130_cap_documentation_set_and_clear_lifecycle` | TEST1130: Documentation set/clear lifecycle must not cross-contaminate cap_description. | cap/definition_test.go:592 |
| test1133 | `Test1133_media_def_def_documentation_lifecycle` | TEST1133: MediaDef set/clear lifecycle for documentation. Setter and clearer must not cross-contaminate the description field. | media/spec_test.go:648 |
| test1142 | `Test1142_resolved_graph_to_mermaid_renders_shapes_dedupes_edges_and_escapes` | TEST1142: ResolvedGraph.to_mermaid() renders node shapes, deduplicates edges, and escapes labels | orchestrator/orchestrator_test.go:68 |
| test1143 | `Test1143_InputItemFromStringDistinguishesGlobDirectoryAndFile` | TEST1143: InputItem::from_string distinguishes glob patterns, directories, and files | input_resolver/types_test.go:11 |
| test1144 | `Test1144_ContentStructureHelpersAndDisplay` | TEST1144: ContentStructure is_list/is_record helpers and Display implementation are correct | input_resolver/types_test.go:43 |
| test1145 | `Test1145_ResolvedInputSetUsesEquivalentMediaAndFileCountCardinality` | TEST1145: ResolvedInputSet uses URN equivalence for common_media and file count for is_sequence | input_resolver/types_test.go:80 |
| test1146 | `Test1146_InputResolverErrorDisplayAndSource` | TEST1146: InputResolverError Display and source() implementations produce correct messages | input_resolver/types_test.go:127 |
| test1147 | `Test1147_machine_syntax_error_display_is_specific` | TEST1147: MachineSyntaxError.Error() includes position and detail. invalidWiringError(7) must produce a message containing "statement 7" and "invalid wiring". | machine/machine_test.go:700 |
| test1148 | `Test1148_machine_parse_error_from_syntax_preserves_variant` | TEST1148: MachineParseError::from(MachineSyntaxError) preserves the syntax error variant | machine/machine_test.go:712 |
| test1149 | `Test1149_machine_parse_error_from_resolution_preserves_variant` | TEST1149: MachineParseError::from(MachineAbstractionError) preserves the resolution error variant | machine/machine_test.go:728 |
| test1150 | `Test1150_add_cap_and_basic_traversal` | TEST1150: Adding one cap creates one edge and makes its output reachable in one step. | planner/live_cap_fab_test.go:634 |
| test1151 | `Test1151_exact_vs_conformance_matching` | TEST1151: Exact target lookup prefers the direct singular or list-producing path over longer alternatives. | planner/live_cap_fab_test.go:662 |
| test1152 | `Test1152_multi_step_path` | TEST1152: Path finding returns the expected two-cap chain through an intermediate media type. | planner/live_cap_fab_test.go:703 |
| test1153 | `Test1153_deterministic_ordering` | TEST1153: Repeated path searches return the same path order for the same graph and target. | planner/live_cap_fab_test.go:724 |
| test1154 | `Test1154_sync_from_caps` | TEST1154: Syncing from caps replaces the existing graph contents with the new cap set. | planner/live_cap_fab_test.go:752 |
| test1155 | `Test1155_FromStrandProducesSingleStrandMachine` | TEST1155: Building a machine from one strand produces one strand with one resolved edge. | machine/machine_test.go:174 |
| test1156 | `Test1156_FromStrandsKeepStrandsDisjoint` | TEST1156: Building from multiple strands keeps them disjoint and preserves input strand order. | machine/machine_test.go:189 |
| test1157 | `Test1157_FromStrandsEmptyInputFailsHard` | TEST1157: Building from zero strands fails with NoCapabilitySteps. | machine/machine_test.go:214 |
| test1158 | `Test1158_MachineIsEquivalentIsStrictPositional` | TEST1158: Machine equivalence is strict about strand order and rejects reordered strands. | machine/machine_test.go:226 |
| test1159 | `Test1159_MachineStrandIsEquivalentWalksNodeBijection` | TEST1159: MachineStrand equivalence accepts two separately built but structurally identical strands. | machine/machine_test.go:248 |
| test1160 | `Test1160_InputOutputAnchors` | TEST1160: Creating a MachineRun stores the canonical notation and starts in the pending state. | machine/machine_test.go:265 |
| test1161 | `Test1161_simple_linear_chain_conversion` | TEST1161: Converting a simple linear plan produces resolved edges for the cap-to-cap chain. | orchestrator/orchestrator_test.go:118 |
| test1162 | `Test1162_heartbeat_frame_with_memory_meta` | TEST1162: Heartbeat frames preserve self-reported memory values stored in metadata. | bifaci/frame_test.go:1335 |
| test1163 | `Test1163_ParseSingleStrandTwoCapsConnectedViaSharedNode` | TEST1163: Parsing one connected strand yields a single machine strand with both caps connected by the shared node. | machine/machine_test.go:371 |
| test1164 | `Test1164_ParseTwoDisconnectedStrandsYieldsTwoMachineStrands` | TEST1164: Parsing two disconnected strand definitions yields two separate machine strands. | machine/machine_test.go:399 |
| test1165 | `Test1165_ParseUnknownCapInRegistryReturnsAbstractionError` | TEST1165: Parsing fails hard when a referenced cap is missing from the registry cache. | machine/machine_test.go:496 |
| test1166 | `Test1166_ParseDuplicateAliasReturnsError` | TEST1166: Duplicate header aliases are reported as syntax errors. | machine/machine_test.go:468 |
| test1167 | `Test1167_ParseUndefinedAliasReturnsError` | TEST1167: Wiring that references an undefined alias is reported as a syntax error. | machine/machine_test.go:483 |
| test1168 | `Test1168_ParseNodeNameCollidesWithCapAlias` | TEST1168: Parsing rejects node names that collide with declared cap aliases. | machine/machine_test.go:511 |
| test1169 | `Test1169_ForEachSetsIsLoop` | TEST1169: Loop markers in notation set the resolved edge loop flag on the following cap step. | machine/machine_test.go:298 |
| test1170 | `Test1170_CollectIsElided` | TEST1170: Parsing and then serializing machine notation round-trips to the canonical form. | machine/machine_test.go:333 |
| test1171 | `Test1171_ParseEmptyInputReturnsError` | TEST1171: Empty machine notation is rejected as a syntax error. | machine/machine_test.go:443 |
| test1172 | `Test1172_MachineStringRepr` | TEST1172: Serializing a two-step strand emits the expected aliases and node names. | machine/machine_test.go:567 |
| test1173 | `Test1173_ToMachineNotationRoundTrips` | TEST1173: Serializing and reparsing a machine preserves strict machine equivalence. | machine/machine_test.go:530 |
| test1175 | `Test1175_EmptyMachineSerializesToEmpty` | TEST1175: Serializing an empty machine produces an empty string. | machine/machine_test.go:559 |
| test1176 | `Test1176_render_payload_json_includes_strand_with_anchors` | TEST1176: ToRenderPayloadJSON for a populated machine includes strand with nodes, edges, input_anchor_nodes, and output_anchor_nodes. | machine/machine_test.go:1051 |
| test1177 | `Test1177_render_payload_for_empty_machine_has_empty_strands_array` | TEST1177: Rendering payload JSON for an empty machine emits an empty strands array. | machine/machine_test.go:1090 |
| test1178 | `Test1178_match_single_source_picks_unique_arg` | TEST1178: One source is assigned to the single compatible cap argument. | machine/machine_test.go:774 |
| test1179 | `Test1179_match_more_specific_source_assigned_to_general_arg` | TEST1179: Source-to-arg matching assigns a more specific source to a compatible general argument. | machine/machine_test.go:787 |
| test1180 | `Test1180_match_unmatched_source_fails_hard` | TEST1180: Matching fails when a source does not conform to any cap input argument. | machine/machine_test.go:800 |
| test1181 | `Test1181_match_two_sources_disambiguated_by_specificity` | TEST1181: matchSourcesToArgs disambiguates two sources by specificity. | machine/machine_test.go:811 |
| test1182 | `Test1182_match_ambiguous_when_two_sources_could_swap` | TEST1182: Matching fails as ambiguous when two sources can be swapped at equal minimum cost. | machine/machine_test.go:835 |
| test1183 | `Test1183_match_more_sources_than_args_fails_hard` | TEST1183: Matching fails when more sources are provided than the cap has input arguments. | machine/machine_test.go:846 |
| test1184 | `Test1184_resolve_strand_single_cap_produces_one_edge` | TEST1184: Resolving a strand with one cap produces one resolved machine edge. | machine/machine_test.go:857 |
| test1185 | `Test1185_resolve_strand_chained_caps_share_intermediate_node` | TEST1185: Resolving a chained strand reuses the intermediate node between adjacent caps. | machine/machine_test.go:888 |
| test1186 | `Test1186_resolve_strand_foreach_marks_following_cap_as_loop` | TEST1186: Resolving a strand with ForEach marks the following cap edge as a loop. | machine/machine_test.go:916 |
| test1187 | `Test1187_StrandNonEquivalenceDifferentCap` | TEST1187: Strand resolution fails when a referenced cap is not found in the registry. | machine/machine_test.go:631 |
| test1188 | `Test1188_resolve_strand_no_cap_steps_fails_hard` | TEST1188: Strand resolution fails when the strand contains no capability steps. | machine/machine_test.go:966 |
| test1189 | `Test1189_StrandEquivalenceWithDifferentNodeAllocationOrders` | TEST1189: Strand resolution keeps canonical anchor ordering stable across equivalent inputs. | machine/machine_test.go:584 |
| test1190 | `Test1190_resolve_strand_inverse_format_converters_no_cycle` | TEST1190: resolveStrand with inverse format converters produces 3 distinct nodes, no cycle. | machine/machine_test.go:979 |
| test1191 | `Test1191_resolve_strand_disbind_pdf_with_file_path_slot_identity` | TEST1191: Disbinding a PDF with a file-path slot preserves the expected identity of the slot binding. | machine/machine_test.go:1012 |
| test1221 | `Test1221_RefineWithMatchingAdapter` | TEST1221: Matching value adapters refine the base media URN when the value fits. | input_resolver/value_adapter_test.go:46 |
| test1222 | `Test1222_RefineNoMatchingAdapter` | TEST1222: Base URNs without a registered adapter are returned unchanged. | input_resolver/value_adapter_test.go:59 |
| test1223 | `Test1223_RefineAdapterReturnsNone` | TEST1223: Adapters that decline to refine leave the original media URN intact. | input_resolver/value_adapter_test.go:70 |
| test1224 | `Test1224_RefineLongestPrefixMatch` | TEST1224: When multiple adapter prefixes match, the longest prefix wins. | input_resolver/value_adapter_test.go:81 |
| test1225 | `Test1225_EmptyRegistry` | TEST1225: An empty value adapter registry returns the input media URN unchanged. | input_resolver/value_adapter_test.go:94 |
| test1226 | `Test1226_HasAdapter` | TEST1226: Adapter presence checks report only the prefixes that were registered. | input_resolver/value_adapter_test.go:103 |
| test1228 | `Test1228_ValueAdapterRefineMatch` | TEST1228: Value adapters can append a more specific marker when both base URN and value match. | input_resolver/value_adapter_test.go:116 |
| test1229 | `Test1229_ValueAdapterRefineNoMatchBase` | TEST1229: Value adapters return no refinement when the base media URN is outside their domain. | input_resolver/value_adapter_test.go:128 |
| test1230 | `Test1230_ValueAdapterRefineNoMatchValue` | TEST1230: Value adapters return no refinement when the inspected value does not match. | input_resolver/value_adapter_test.go:137 |
| test1236 | `Test1236_Disc2ModelSpecValidationPatternFiltersContent` | TEST1236: Colon-delimited model spec text survives TXT candidate discrimination. | input_resolver/resolver_test.go:202 |
| test1237 | `Test1237_Disc5EmptyCandidates` | TEST1237: Empty candidates → empty result | input_resolver/resolver_test.go:230 |
| test1238 | `Test1238_Disc6UnknownUrnSurvives` | TEST1238: Unknown URN survives discrimination | input_resolver/resolver_test.go:239 |
| test1256 | `Test1256_parse_simple_machine` | TEST1256: A single declared cap and one wiring parse into a two-node one-edge DAG. | orchestrator/orchestrator_test.go:254 |
| test1257 | `Test1257_parse_two_step_chain` | TEST1257: Two sequential wirings preserve the intermediate node media type. | orchestrator/orchestrator_test.go:281 |
| test1261 | `Test1261_cap_not_found_in_registry` | TEST1261: Parsing fails when a declared cap is absent from the registry. In Go the machine parser resolves caps before the orchestrator layer checks, so the error may be ErrMachineSyntaxParseFailed or ErrCapNotFound. | orchestrator/orchestrator_test.go:316 |
| test1262 | `Test1262_invalid_machine_notation` | TEST1262: Non-machine text fails with a machine syntax parse error. | orchestrator/orchestrator_test.go:334 |
| test1263 | `Test1263_cycle_detection` | TEST1263: Cyclic wirings are rejected as non-DAG orchestrations. In Go the machine parser may reject cycles at the parse layer or the orchestrator layer. | orchestrator/orchestrator_test.go:351 |
| test1264 | `Test1264_incompatible_media_types_at_shared_node` | TEST1264: Shared nodes with incompatible upstream and downstream media fail during parsing. | orchestrator/orchestrator_test.go:375 |
| test1265 | `Test1265_compatible_media_urns_at_shared_node` | TEST1265: Shared nodes accept compatible media URNs when one is a more specific form of the other. | orchestrator/orchestrator_test.go:397 |
| test1267 | `Test1267_structure_match_both_record` | TEST1267: Record-shaped outputs can feed record-shaped inputs without error. | orchestrator/orchestrator_test.go:415 |
| test1268 | `Test1268_structure_match_both_opaque` | TEST1268: Opaque outputs can feed opaque inputs without triggering structure conflicts. | orchestrator/orchestrator_test.go:433 |
| test1269 | `Test1269_parse_multiline_machine` | TEST1269: Multi-line machine notation parses successfully with the same semantics as inline notation. | orchestrator/orchestrator_test.go:451 |
| test1271 | `Test1271_media_adapter_selection_constant` | TEST1271: MEDIA_ADAPTER_SELECTION constant parses and has expected tags | standard/caps_test.go:134 |
| test1272 | `Test1272_adapter_cap_constant_parses` | TEST1272: CAP_ADAPTER_SELECTION constant parses as a valid CapUrn | standard/caps_test.go:146 |
| test1273 | `Test1273_adapter_selection_urn_builder` | TEST1273: CapAdapterSelection has correct in/out specs (in=media: out=media:adapter-selection;json;record) | standard/caps_test.go:156 |
| test1275 | `Test1275_adapter_selection_dispatchable_by_specific_provider` | TEST1275: A cap whose output is adapter-selection can dispatch adapter-selection requests; identity (wildcard output) cannot, because wildcard output cannot satisfy a specific output requirement. | standard/caps_test.go:170 |
| test1276 | `Test1276_RegisterNonConflicting` | TEST1276: Registration of a cap group with non-conflicting adapters succeeds | input_resolver/adapter_registry_test.go:30 |
| test1277 | `Test1277_RejectConformingOverlap` | TEST1277: Registration of a cap group with an adapter that conforms_to an existing adapter is rejected | input_resolver/adapter_registry_test.go:47 |
| test1278 | `Test1278_RejectEntireGroup` | TEST1278: Registration rejects the entire group — no partial registration | input_resolver/adapter_registry_test.go:69 |
| test1279 | `Test1279_IntraGroupConflict` | TEST1279: Intra-group conflict (two adapters within same group overlap) is rejected | input_resolver/adapter_registry_test.go:98 |
| test1280 | `Test1280_FindAdaptersForExtension` | TEST1280: find_adapters_for_extension returns correct cartridge IDs | input_resolver/adapter_registry_test.go:118 |
| test1281 | `Test1281_NoAdapterForUnknown` | TEST1281: has_adapter_for_extension returns false for unregistered extension | input_resolver/adapter_registry_test.go:136 |
| test1282 | `Test1282_adapter_selection_auto_registered` | TEST1282: AdapterSelectionOp is auto-registered by CartridgeRuntime | bifaci/cartridge_runtime_test.go:3203 |
| test1283 | `Test1283_adapter_selection_custom_override` | TEST1283: Custom adapter selection Op overrides the default | bifaci/cartridge_runtime_test.go:3217 |
| test1284 | `Test1284_cap_group_with_adapter_urns` | TEST1284: Cap group with adapter URNs serializes and deserializes correctly | bifaci/manifest_test.go:348 |
| test1285 | `Test1285_ConfirmedNoAdaptersFails` | TEST1285: detect_file_confirmed fails when no adapters are registered for the extension | input_resolver/resolver_test.go:273 |
| test1286 | `Test1286_ConfirmedAdapterReturnsUrns` | TEST1286: detect_file_confirmed succeeds when adapter returns URNs | input_resolver/resolver_test.go:291 |
| test1287 | `Test1287_ConfirmedAllAdaptersNoMatch` | TEST1287: detect_file_confirmed fails when all adapters return empty END (no match) | input_resolver/resolver_test.go:318 |
| test1288 | `Test1288_StructureFromMarkerTags` | TEST1288: structure_from_marker_tags correctly maps tag combinations to ContentStructure | input_resolver/resolver_test.go:249 |
| test1289 | `Test1289_bfs_reachable_includes_source_roundtrip` | TEST1289: BFS reachable targets includes the source itself when round-trip paths exist. A→B and B→A means A is reachable from A (via A→B→A). | planner/live_cap_fab_test.go:437 |
| test1290 | `Test1290_iddfs_finds_roundtrip_paths` | TEST1290: IDDFS find_paths_to_exact_target finds round-trip paths when source == target. | planner/live_cap_fab_test.go:471 |
| test1291 | `Test1291_iddfs_roundtrip_with_sequence` | TEST1291: IDDFS round-trip paths are also found with is_sequence=true. The ForEach/Collect edges must not block round-trip discovery. | planner/live_cap_fab_test.go:508 |
| test1292 | `Test1292_bfs_iddfs_roundtrip_consistency` | TEST1292: BFS and IDDFS agree that round-trip targets exist. If BFS says target X is reachable from source X, IDDFS must find at least one path. | planner/live_cap_fab_test.go:537 |
| test1293 | `Test1293_roundtrip_requires_cap_steps` | TEST1293: IDDFS round-trip does not produce paths with 0 cap steps. Identity-only round trips (no real transformation) must be excluded. | planner/live_cap_fab_test.go:578 |
| test1294 | `Test1294_rule11_void_input_with_stdin_rejected` | TEST1294: RULE11 - void-input cap with stdin source rejected | cap/validation_test.go:296 |
| test1295 | `Test1295_rule11_non_void_input_without_stdin_rejected` | TEST1295: RULE11 - non-void-input cap without stdin source rejected | cap/validation_test.go:309 |
| test1296 | `Test1296_rule11_void_input_cli_flag_only_passes` | TEST1296: RULE11 - void-input cap with only cli_flag sources passes | cap/validation_test.go:322 |
| test1297 | `Test1297_rule11_non_void_input_with_stdin_passes` | TEST1297: RULE11 - non-void-input cap with stdin source passes | cap/validation_test.go:334 |
| test1800 | `Test1800_kind_identity_only_for_bare_cap` | TEST1800: Identity classifier — only explicit effect=none qualifies. `cap:effect=none` is the fully generic identity on every axis; adding any tag (even one that doesn't constrain in/out) demotes the cap to Transform because the operation/metadata axis is no longer fully generic. | urn/cap_urn_test.go:1585 |
| test1801 | `Test1801_kind_source_when_input_is_void` | TEST1801: Source classifier — in=media:void, out non-void. The y dimension may carry any tags; void on the input alone is what matters. | urn/cap_urn_test.go:1615 |
| test1802 | `Test1802_kind_sink_when_output_is_void` | TEST1802: Sink classifier — out=media:void, in non-void. | urn/cap_urn_test.go:1630 |
| test1803 | `Test1803_kind_effect_when_both_sides_void` | TEST1803: Effect classifier — both sides void. Reads as `() → ()`. | urn/cap_urn_test.go:1645 |
| test1804 | `Test1804_kind_transform_for_normal_data_processors` | TEST1804: Transform classifier — at least one side non-void, and the cap is not the bare identity. The default kind for ordinary data-processing caps. | urn/cap_urn_test.go:1660 |
| test1805 | `Test1805_kind_invariant_under_canonical_spellings` | TEST1805: Kind is invariant under canonicalization. The same morphism written in many surface forms must classify the same way once parsed. This pins the rule that kind is a property of the cap as a structured object, not of any particular spelling. | urn/cap_urn_test.go:1675 |
| test1810 | `Test1810_media_void_is_atomic` | TEST1810: media:void is atomic — refinements are parse errors. Mirrored across every language port (Rust, Go, Python, Swift/ObjC, JS) under the SAME number. Any divergence is a wire-level inconsistency — the unit type's atomicity is part of the protocol's deepest layer, not a per-port detail. The bare `media:void` parses successfully; any combination with another tag (marker or key=value) MUST fail with VoidNotAtomic. This forecloses a fake taxonomy of unit values; reasons or labels for *why* void is used belong on the cap URN's non-directional tags or in cap args. | urn/media_urn_test.go:724 |
| test1820 | `Test1820_specificity_question_is_zero` | TEST1820: A `?`-valued cap-tag scores 0. Same as missing. | urn/cap_urn_test.go:1725 |
| test1821 | `Test1821_specificity_must_not_have_is_five` | TEST1821: A `!`-valued cap-tag scores 5 (top of negative chain). | urn/cap_urn_test.go:1737 |
| test1822 | `Test1822_specificity_must_have_any_is_two` | TEST1822: A `*`-valued cap-tag (including bare markers) scores 2. | urn/cap_urn_test.go:1745 |
| test1823 | `Test1823_specificity_exact_value_is_four` | TEST1823: An exact-valued cap-tag scores 4. | urn/cap_urn_test.go:1761 |
| test1824 | `Test1824_specificity_combined_y_axis` | TEST1824: All six forms compose additively on a single cap. This pins the truth-table sum across the y axis as a whole. | urn/cap_urn_test.go:1769 |
| test1830 | `Test1830_canonicalize_no_constraint` | TEST1830: ?x ≡ x? ≡ x=? all canonicalize to ?x. | urn/cap_urn_test.go:1787 |
| test1831 | `Test1831_canonicalize_absent_or_not_value` | TEST1831: ?x=v and x?=v both canonicalize to x?=v. The third hypothetical form `x=?v` is NOT recognized as a qualifier — a value starting with `?` is just an exact value beginning with a `?` character. | urn/cap_urn_test.go:1798 |
| test1832 | `Test1832_canonicalize_must_have_any` | TEST1832: x ≡ x=* both canonicalize to bare x. | urn/cap_urn_test.go:1818 |
| test1833 | `Test1833_canonicalize_present_not_value` | TEST1833: !x=v and x!=v both canonicalize to x!=v. The third hypothetical form `x=!v` is NOT recognized as a qualifier — a value starting with `!` is just an exact value beginning with a `!` character. | urn/cap_urn_test.go:1829 |
| test1834 | `Test1834_canonicalize_exact_value` | TEST1834: x=v stays as x=v (the lone exact-value form). | urn/cap_urn_test.go:1849 |
| test1835 | `Test1835_canonicalize_must_not_have` | TEST1835: !x ≡ x! ≡ x=! all canonicalize to !x. | urn/cap_urn_test.go:1856 |
| test1842 | `Test1842_truth_table_full_cross_product` | TEST1842: Full 6×6 truth table — every cell must match the matrix in 04-PREDICATES.md §2.5. Treats prefix `cap:` as the host for a single-key URN (key `x`), pairing every instance form with every pattern form. | urn/cap_urn_test.go:1867 |
| test1845 | `Test1845_axis_weighting_in_dominates_y` | TEST1845: With equal out-axis, in-axis dominates over y-axis. | urn/cap_urn_test.go:1935 |
| test1847 | `Test1847_cartridge_build_legacy_package_fallback` | TEST1847: A build from a registry manifest published BEFORE `packages[]` existed carries only the legacy singular `package` (no `format`). It must still deserialize (a missing `packages` must not fail the whole parse) and `primary_package()` must fall back to that legacy package, so a registry not yet republished with the dual-write keeps installing. When `packages[]` is present it is preferred over the legacy field. | bifaci/cartridge_repo_test.go:1203 |
| test1849 | `Test1849_resolve_for_host_compatible_latest` | TEST1849: latest version has a host build → Compatible, resolving to the latest version and that platform's native-format package. | bifaci/cartridge_repo_test.go:1327 |
| test1850 | `Test1850_resolve_for_host_compatible_outdated` | TEST1850: the latest version lacks a host build but an older version has one → CompatibleOutdated, resolving to the older version with a reason naming both the latest and the resolved version. | bifaci/cartridge_repo_test.go:1360 |
| test1851 | `Test1851_resolve_for_host_incompatible` | TEST1851: no version ships a host build → Incompatible, no resolved version/package, reason states the host platform. | bifaci/cartridge_repo_test.go:1395 |
| test1852 | `Test1852_resolve_for_host_skips_build_with_no_installer` | TEST1852: a host build whose packages[] is empty AND has no legacy `package` ships no installer; resolution must SKIP it (not resolve to an un-downloadable version) and fall through to an older usable version. | bifaci/cartridge_repo_test.go:1417 |
| test1853 | `Test1853_host_platform_normalized_form` | TEST1853: host_platform() returns a normalized {os}-{arch} string with arch aarch64 mapped to arm64 — the exact form the registry uses. | bifaci/cartridge_repo_test.go:1443 |
| test1872 | `Test1872_registry_url_from_build_env_passes_through_nonempty` | TEST1872: `registry_url_from_build_env` passes a non-empty registry URL through unchanged. This is the function that decides the engine's baked PRIMARY registry (surfaced over SystemService.HealthStatus); a published build must report exactly the URL it was compiled with. | bifaci/manifest_test.go:376 |
| test1873 | `Test1873_registry_url_from_build_env_none_for_dev` | TEST1873: an unset build-env value (nil) yields nil — a dev build has no baked registry, so the engine reports an empty primary-registry URL and loads only `dev/` cartridges (mirror of Rust test1873). | bifaci/manifest_test.go:389 |
| test1874 | `Test1874_registry_url_from_build_env_rejects_empty_string` | TEST1874: an exported-but-empty value (a pointer to "") is neither a dev build nor a valid identity and MUST fail hard, so the build can never silently hash the empty string into a fake registry slug. We assert the panic AND its exact message, so a regression that dropped the check (or replaced it with a silent fallback) is caught rather than passing on a bogus empty primary registry (mirror of Rust test1874). | bifaci/manifest_test.go:399 |
| test1875 | `Test1875_scan_all_reaches_both_dev_and_registry_slugs` | TEST1875: scan-all — a registry slug folder AND the dev slot present on disk are BOTH scanned, regardless of the host's own baked registry. The dev cartridge (null registry under dev/) and the registry cartridge (its url hashing to its slug folder) each reach their probe. Both fixtures lack a real bifaci binary, so both end at HandshakeFailed — proving discovery REACHED them (was not filtered out by a registry pin), which is the behavior under test. A registry-pin rejection would instead surface BadInstallation and never probe. | bifaci/cartridge_discovery_test.go:116 |
| test1876 | `Test1876_other_channel_subtree_is_skipped` | TEST1876: only the host's channel subtree is scanned. A cartridge under a slug's `release/` folder is invisible to a nightly host even though the slug folder is present (its `nightly/` subtree is absent). | bifaci/cartridge_discovery_test.go:144 |
| test1877 | `Test1877_registry_cartridge_under_wrong_slug_is_bad_install` | TEST1877: a registry cartridge hand-copied under the WRONG registry slug folder fails the three-place rule (BadInstallation) — scan-all does not mean "accept anywhere", placement must still be self-consistent. | bifaci/cartridge_discovery_test.go:157 |
| test1878 | `Test1878_bundled_provider_without_baked_hash_is_rejected` | TEST1878: a cartridge marked `installed_from: bundle` with no baked hash in BUNDLED_PROVIDER_HASHES (the const is empty under plain `cargo test`) is rejected as BadInstallation — the bundled-integrity gate fires before the probe. Proves the verify is wired into discovery; a real bundle build bakes the hash so the matching directory passes. Non-macOS only: on macOS the baked-hash path is intentionally absent (OS code-signature is the guard), so a bundled provider is accepted there and would instead end at the probe. | bifaci/cartridge_discovery_test.go:170 |
| test1879 | `Test1879_sync_roster_adds_and_removes_registered_dir_live` | TEST1879: SyncRoster updates the LIVE host inventory in place — the engine sees an added registered-dir cartridge via a fresh RelayNotify without reconnecting, and a subsequent empty sync removes it. This is the macOS-XPC `syncDiscoveryOutcomes` parity path the daemon uses after a registry verdict flips a held cartridge to Listed. | bifaci/host_roster_test.go:63 |
| test1880 | `Test1880_AliasNameNormalizationRules` | TEST1880: alias name normalization lowercases and accepts the allowed character class; rejects colon, whitespace, and out-of-class chars with the right error. A broken validator would let a URN-shaped or whitespace name through, or mangle a valid name. | machine/alias_test.go:18 |
| test1881 | `Test1881_TokenURNvsAliasDetection` | TEST1881: URN-vs-alias detection keys purely on the presence of ':'. The whole design rests on this discriminator being exact. | machine/alias_test.go:34 |
| test1882 | `Test1882_ClassifyAliasTargetByPrefix` | TEST1882: alias target classification distinguishes cap from media by prefix and rejects a non-URN target. The typed-boundary enforcement in the registry depends on this. | machine/alias_test.go:43 |
| test1883 | `Test1883_CapPositionAliasResolvesToCap` | Test1883: a cap-position name with no local header resolves as a cap alias. | machine/alias_test.go:166 |
| test1884 | `Test1884_LocalHeaderShadowsCapAlias` | Test1884: a local header alias shadows a fabric alias of the same name. | machine/alias_test.go:177 |
| test1885 | `Test1885_CapPositionAliasToMediaIsError` | Test1885: a cap-position alias that resolves to a MEDIA URN is a hard error. | machine/alias_test.go:190 |
| test1886 | `Test1886_UnregisteredCapNameIsUndefinedAlias` | TEST1886: a cap-position name that is neither a local header nor a registered alias still raises UndefinedAlias. The alias mechanism must not mask a genuinely undefined name. | machine/alias_test.go:202 |
| test1887 | `Test1887_ManifestSerdeRoundTripsAliases` | Test1887: the Manifest type round-trips an `aliases` map. | machine/alias_test.go:57 |
| test1888 | `Test1888_ResolveAliasReturnsTarget` | TEST1888: resolve_alias returns the alias target untyped. Seeding a media alias and resolving it yields the media URN; a malformed alias name is rejected before any lookup. | machine/alias_test.go:74 |
| test1889 | `Test1889_ResolveAliasTypedEnforcesKind` | Test1889: resolve alias typed enforces the expected kind. | machine/alias_test.go:91 |
| test1890 | `Test1890_GetCapViaAliasAndTypeMismatch` | TEST1890: get_cap accepts a cap alias and returns the aliased cap; a media alias passed to get_cap fails hard (typed boundary). This proves alias substitution AND type enforcement at the registry's cap surface. | machine/alias_test.go:104 |
| test1891 | `Test1891_GetMediaDefViaAliasAndTypeMismatch` | TEST1891: get_media_def accepts a media alias and returns the aliased spec; a cap alias passed to get_media_def fails hard. | machine/alias_test.go:121 |
| test1892 | `Test1892_UnknownAliasIsNotFound` | TEST1892: an unknown alias name is a hard not-found, never a silent empty; unknown and malformed names are treated the same. This is the "expose issues, no fallback" contract. | machine/alias_test.go:141 |
| test6182 | `Test6182_InputValidator_WithSchemaValidation` | TEST6182: Input validator  with schema validation | cap/schema_validation_test.go:296 |
| test6183 | `Test6183_OutputValidator_WithSchemaValidation` | TEST6183: Output validator  with schema validation | cap/schema_validation_test.go:356 |
| test6184 | `Test6184_CapValidationCoordinator_EndToEnd` | TEST6184: Cap validation coordinator  end to end | cap/schema_validation_test.go:415 |
| test6185 | `Test6185_FileSchemaResolver_ErrorHandling` | TEST6185: File schema resolver  error handling | cap/schema_validation_test.go:530 |
| test6186 | `Test6186_RegistryGetCap` | TEST136 (deleted): exercised the private `cacheKey` method on the unified FabricRegistry. The on-disk cache filename scheme is an implementation detail of the persistence layer; equivalent observable behavior — that two equivalent URNs land in the same cache slot — is covered by Test140 (`same_cap_different_spellings_same_url`). Rust and Python dropped this; this deletion keeps the Go mirror in parity. | cap/registry_test.go:40 |
| test6202 | `Test6202_empty_cap_urn_defaults_to_wildcard` | TEST6202: Bare top cap is illegal; identity must be explicit effect=none | urn/cap_urn_test.go:614 |
| test6207 | `Test6207_ConcatenatedVsFinalPayloadDivergence` | Mirror-specific coverage: Test that concatenated() returns full payload while final_payload() returns only last chunk | bifaci/integration_test.go:1692 |
| test6210 | `Test6210_ResponseWrapperFromText` | TEST6210: Response wrapper from text | cap/response_test.go:45 |
| test6214 | `Test6214_ResponseWrapperAsFloat` | TEST6214: Response wrapper as float | cap/response_test.go:96 |
| test6218 | `Test6218_ResponseWrapperAsBool` | TEST6218: Response wrapper as bool | cap/response_test.go:111 |
| test6222 | `Test6222_ResponseWrapperIsEmpty` | TEST6222: Response wrapper is empty | cap/response_test.go:142 |
| test6226 | `Test6226_ResponseWrapperGetContentType` | TEST6226: Response wrapper get content type | cap/response_test.go:153 |
| test6230 | `Test6230_ResponseWrapperMatchesOutputType` | TEST6230: Response wrapper matches output type | cap/response_test.go:165 |
| test6234 | `Test6234_ResponseWrapperValidateAgainstCap` | TEST6234: Response wrapper validate against cap | cap/response_test.go:251 |
| test6238 | `Test6238_CapRequestHandling` | Additional existing tests below (not part of TEST108-116 sequence) | cap/definition_test.go:450 |
| test6245 | `Test6245_CapDescription` | TEST6245: Cap description | cap/definition_test.go:467 |
| test6248 | `Test6248_CapWithMediaDefs` | TEST6248: Cap with media defs | cap/definition_test.go:480 |
| test6251 | `Test6251_cap_version_zero_round_trip` | TEST6251: Cap.Version zero value is omitted on serialize and defaults to 0 on deserialize. | cap/definition_test.go:613 |
| test6255 | `Test6255_cap_version_nonzero_round_trip` | TEST6255: Cap.Version non-zero value round-trips through JSON serialize/deserialize. | cap/definition_test.go:630 |
| test6259 | `Test6259_CapJSONRoundTrip` | TEST6259: Cap j s o n round trip | cap/definition_test.go:647 |
| test6263 | `Test6263_SchemaValidator_ValidateArgumentWithSchema_NilSchema` | Additional Go-specific coverage: nil schema skips direct schema validation | cap/schema_validation_test.go:97 |
| test6267 | `Test6267_SchemaValidator_ValidateOutputWithSchema_Failure` | TEST6267: Schema validator  validate output with schema  failure | cap/schema_validation_test.go:148 |
| test6271 | `Test6271_SchemaValidator_ValidateArguments_Integration` | TEST6271: Schema validator  validate arguments  integration | cap/schema_validation_test.go:180 |
| test6274 | `Test6274_SchemaValidator_ArraySchemaValidation` | TEST6274: Schema validator  array schema validation | cap/schema_validation_test.go:244 |
| test6314 | `Test6314_ComplexNestedSchemaValidation` | TEST6314: Complex nested schema validation | cap/schema_validation_test.go:542 |
| test6317 | `Test6317_MediaUrnResolutionWithRegistry` | TEST6317: Media urn resolution with registry | cap/schema_validation_test.go:621 |
| test6320 | `Test6320_CustomMediaUrnResolution` | TEST6320: Custom media urn resolution | cap/schema_validation_test.go:656 |
| test6325 | `Test6325_RegistryValidation` | TEST6325: Registry validation | cap/registry_test.go:54 |
| test6329 | `Test6329_CacheOperations` | TEST6329: Cache operations | cap/registry_test.go:69 |
| test6330 | `Test6330_ChunkingDataIntegrity3x` | Mirror-specific coverage: Test auto-chunking preserves data integrity across chunk boundaries for 3x max_chunk payload | bifaci/integration_test.go:1716 |
| test6339 | `Test6339_CapUrn_JSONSerialization` | JSON serialization test (not numbered in Rust) | urn/cap_urn_test.go:1421 |
| test6343 | `Test6343_ParseSimple` | Mirror-specific coverage: Test parsing simple media URN verifies correct structure with no version, subtype, or profile | urn/media_urn_test.go:14 |
| test6347 | `Test6347_ParseWithSubtype` | Mirror-specific coverage: Test parsing media URN with marker tags works correctly | urn/media_urn_test.go:22 |
| test6351 | `Test6351_ParseWithProfile` | Mirror-specific coverage: Test parsing media URN with profile extracts profile URL correctly | urn/media_urn_test.go:32 |
| test6355 | `Test6355_Constructor` | Mirror-specific coverage: Test simple constructor creates media URN with type tag | urn/media_urn_test.go:203 |
| test6359 | `Test6359_WithSubtypeConstructor` | Mirror-specific coverage: Test with_subtype constructor creates media URN with subtype | urn/media_urn_test.go:210 |
| test6363 | `Test6363_CapManifestWithPageURL` | TEST6363: Cap manifest with page u r l | bifaci/manifest_test.go:62 |
| test6367 | `Test6367_CapManifestValidation` | TEST6367: Cap manifest validation | bifaci/manifest_test.go:255 |
| test6371 | `Test6371_CapManifestCompatibility` | TEST6371: Cap manifest compatibility | bifaci/manifest_test.go:285 |
| test6374 | `Test6374_CartridgeAttachmentErrorKindMatchesProtoSnakeCase` | Test6374_CartridgeAttachmentErrorKindMatchesProtoSnakeCase pins every variant's string value against its proto snake_case name. New variants must be added here AND in the Rust / Swift / proto sides. | bifaci/relay_switch_test.go:850 |
| test6379 | `Test6379_CartridgeAttachmentErrorJSONRoundTrips` | Test6379_CartridgeAttachmentErrorJSONRoundTrips verifies a CartridgeAttachmentError marshals to JSON and unmarshals back without changing the kind for every variant. RelayNotify wire payload is JSON; a single-variant regression breaks the entire per-master parse. | bifaci/relay_switch_test.go:878 |
| test6382 | `Test6382_parse_registry_json` | TEST6382: Test parsing registry JSON without stdin args verifies cap structure | cap/registry_test.go:79 |
| test6388 | `Test6388_per_cap_url_uses_sha256` | TEST6388: Per-cap URL is /caps/<sha256-hex> — no URN-grammar characters in the path, no percent-encoding gymnastics. | cap/registry_test.go:135 |
| test6391 | `Test6391_same_cap_different_spellings_same_url` | TEST6391: Equivalent URNs (different tag order, etc.) hash to the same key. This is the property that makes cross-language lookups land at the same registry object regardless of which capdag implementation issued the request. | cap/registry_test.go:149 |
| test6423 | `Test6423_CartridgeAttachmentErrorDecodesProtoSnakeCaseStrings` | Test6423_CartridgeAttachmentErrorDecodesProtoSnakeCaseStrings is the engine→Go-host (or Swift→Go-host) decode path: incoming JSON uses the snake_case wire format, and the Go side must resolve each string into the matching variant. CartridgeAttachmentErrorKind is just `type ... string`, so this test is also a check that the JSON unmarshaller doesn't normalise/lowercase/etc the bytes behind our backs. | bifaci/relay_switch_test.go:926 |
| test6428 | `Test6428_IntegrationVersionlessCapCreation` | Test6428_IntegrationVersionlessCapCreation verifies caps can be created without version fields | bifaci/integration_test.go:47 |
| test6431 | `Test6431_IntegrationCaseInsensitiveUrns` | Test6431_IntegrationCaseInsensitiveUrns verifies URNs are case-insensitive | bifaci/integration_test.go:75 |
| test6433 | `Test6433_IntegrationCapValidation` | Test6433_IntegrationCapValidation verifies cap schema validation | bifaci/integration_test.go:119 |
| test6524 | `Test6524_LogFramesDuringRequest` | Mirror-specific coverage: Test LOG frames sent during a request are transparently skipped by host | bifaci/integration_test.go:561 |
| test6526 | `Test6526_HostInitiatedHeartbeatNoPingPong` | Mirror-specific coverage: Test host does not echo back cartridge's heartbeat response (no infinite ping-pong) | bifaci/integration_test.go:949 |
| test6528 | `Test6528_CartridgeSuddenDisconnect` | Mirror-specific coverage: Test host receives error when cartridge closes connection unexpectedly | bifaci/integration_test.go:1106 |
| test6529 | `Test6529_EndFrameNoPayload` | Mirror-specific coverage: Test END frame without payload is handled as complete response with empty data | bifaci/integration_test.go:1214 |
| test6530 | `Test6530_StreamingSequenceNumbers` | Mirror-specific coverage: Test streaming response sequence numbers are contiguous and start from 0 | bifaci/integration_test.go:1270 |
| test6531 | `Test6531_RequestAfterShutdown` | Mirror-specific coverage: Test host request on a closed host returns error | bifaci/integration_test.go:1346 |
| test6532 | `Test6532_ArgumentsMultiple` | Mirror-specific coverage: Test multiple arguments are correctly serialized in CBOR payload | bifaci/integration_test.go:1389 |
| test6533 | `Test6533_AutoChunkingReassembly` | Mirror-specific coverage: Test auto-chunking splits payload larger than max_chunk into CHUNK frames + END frame, and host concatenated() reassembles the full original data | bifaci/integration_test.go:1462 |
| test6535 | `Test6535_ExactMaxChunkSingleEnd` | Mirror-specific coverage: Test payload exactly equal to max_chunk produces single END frame (no CHUNK frames) | bifaci/integration_test.go:1548 |
| test6537 | `Test6537_MaxChunkPlusOneSplitsIntoTwo` | Mirror-specific coverage: Test payload of max_chunk + 1 produces exactly one CHUNK frame + one END frame | bifaci/integration_test.go:1613 |
| test6586 | `Test6586_FilePathArrayInvalidJSONFails` | TEST6586: A scalar file-path arg receiving a nonexistent path fails hard with a clear error that names the path. The runtime refuses to silently swallow user mistakes like typos or wrong directories. | bifaci/cartridge_runtime_test.go:1290 |
| test6587 | `Test6587_FilePathArrayOneFileMissingFailsHard` | TEST6587: file-path-array with literal nonexistent path fails hard | bifaci/cartridge_runtime_test.go:1332 |
| test6588 | `Test6588_FilePathArrayEmptyArray` | TEST6588: file-path arg in CBOR mode with empty Array returns empty. CBOR Array (not JSON-encoded) is the multi-input wire form for sequence args. Mirrors Rust test351_file_path_array_empty_array. | bifaci/cartridge_runtime_test.go:1642 |
| test6594 | `Test6594_capabilities_empty_initially` | TEST6594: capabilities() returns empty JSON initially (no running cartridges) | bifaci/host_multi_test.go:70 |
| test6597 | `Test6597_write_chunked_chunk_index_ordering` | TEST6597: WriteResponseWithChunking splits payload into exactly N chunks per max_chunk, and chunk_index tracks ordering within the stream (0, 1, 2, ...). Note: Go assigns seq at write time (Rust assigns seq=0 and uses SeqAssigner at output stage; Go inlines the seq assignment into the write path instead). | bifaci/io_test.go:1156 |
| test6602 | `Test6602_codec_key_constants` | TEST6602: Codec key constants match protocol spec values | bifaci/frame_test.go:1538 |
| test6605 | `Test6605_insert_schema_populates_cache` | TEST6605: InsertSchema seeds the cache so subsequent validation hits a real compiled schema rather than the skip-on-unknown path. A registry that silently dropped inserts would let validation calls return nil even for inputs that violate the schema. | media/profile_test.go:72 |
| test6606 | `Test6606_registry_creation` | TEST6606: A freshly constructed registry over a temp cache dir is operational: the cache directory exists on disk and the registry is usable. Inserting then reopening a registry on the same directory must load the persisted schema — this genuinely exercises the disk-cache round-trip (Rust new_with_cache_dir + load_all_cached_schemas), not just the in-memory map. | media/profile_test.go:113 |
| test6609 | `Test6609_wildcard_empty_cap_defaults_to_media_wildcard` | TEST6609: bare top cap is illegal; identity must be explicit effect=none | urn/cap_urn_test.go:1184 |
| test6610 | `Test6610_wildcard_002_in_only_defaults_out_to_media` | TEST6610: cap:in without a non-vacuous axis/tag is illegal bare top | urn/cap_urn_test.go:1508 |
| test6611 | `Test6611_wildcard_003_out_only_defaults_in_to_media` | TEST6611: cap:out without a non-vacuous axis/tag is illegal bare top | urn/cap_urn_test.go:1515 |
| test6612 | `Test6612_wildcard_005_explicit_asterisk_becomes_media` | TEST6612: explicit wildcard in/out is still illegal bare top | urn/cap_urn_test.go:1529 |
| test6613 | `Test6613_wildcard_006_specific_in_wildcard_out` | TEST6613: top in plus wildcard out is still illegal bare top | urn/cap_urn_test.go:1536 |
| test6614 | `Test6614_wildcard_accepts_specific` | TEST6614: Legal generic cap with marker-only y-axis matches specific caps | urn/cap_urn_test.go:1194 |
| test6615 | `Test6615_specificity_scoring` | TEST6615: Specificity - marker-only wildcard scores on y-axis only | urn/cap_urn_test.go:1206 |
| test6618 | `Test6618_identity_forms_equivalent` | TEST6618: Long and short explicit identity forms are equivalent | urn/cap_urn_test.go:1218 |
| test6622 | `Test6622_identity_routing_isolation` | TEST6622: Cap identity does not route as a declared-effect provider | urn/cap_urn_test.go:1244 |
| test6630 | `Test6630_cardinality_string` | TEST6630: Tests InputCardinality String() representation | planner/cardinality_test.go:133 |
| test6632 | `Test6632_pattern_string` | TEST6632: Tests CardinalityPattern String() representation | planner/cardinality_test.go:140 |
| test6634 | `Test6634_machine_result_primary_output` | TEST6634: Tests MachineResult PrimaryOutput returns populated output and nil when empty Verifies the PrimaryOutput() accessor distinguishes populated vs empty outputs maps | planner/plan_test.go:150 |
| test6635 | `Test6635_iteration_edge_does_not_create_topological_dependency` | TEST6635: Tests that edge types determine dependency direction in TopologicalOrder Iteration edges must NOT create a topological dependency (ForEach body must not block ForEach node). Direct edges MUST create a dependency. Verifies that edge kind affects plan execution order. | planner/plan_test.go:167 |
| test6636 | `Test6636_foreach_body_bounds_determine_extraction` | TEST6636: Tests that ForEach node's body range fields are used correctly by ExtractForEachBody The bodyEntry/bodyExit fields define which nodes are in scope. Verifies that wrong body bounds produce a different extraction than correct ones — body_exit determines what gets included. | planner/plan_test.go:199 |
| test6637 | `Test6637_single_cap_plan_validates_and_orders_correctly` | TEST6637: Tests SingleCap plan passes Validate and TopologicalOrder produces correct sequence Verifies the plan is structurally sound: input_slot must precede cap_0 must precede output | planner/plan_test.go:223 |
| test6638 | `Test6638_merge_strategy_values` | TEST6638: Tests MergeStrategy enum values Verifies MergeConcat and MergeZipWith have correct string representations | planner/plan_test.go:248 |
| test6639 | `Test6639_output_node_registered_on_add` | TEST6639: Tests Output node is automatically registered as output_node on AddNode Verifies that Validate() accepts a plan where the Output node is the plan's only output_node | planner/plan_test.go:255 |
| test6640 | `Test6640_split_and_merge_not_classified_as_cap_fanout_fanin` | TEST6640: Tests that IsCap/IsFanOut/IsFanIn return false for Split and Merge node types Verifies that node type classification methods correctly reject non-cap, non-foreach, non-collect kinds | planner/plan_test.go:291 |
| test6645 | `Test6645_argument_resolution_string_representations` | TEST6645: Tests ArgumentResolution String() returns correct snake_case names ArgumentInfo.Resolution is serialized to JSON using String(). Verifies that each resolution variant maps to the correct identifier expected by API consumers. | planner/plan_builder_test.go:51 |
| test6646 | `Test6646_analyze_path_arguments_stdin_is_from_input_file` | TEST6646: Tests AnalyzePathArguments classifies stdin arg as FromInputFile for first cap Verifies that the argument analysis correctly identifies input-file arguments when the cap's stdin arg media URN matches the cap's in_spec. | planner/plan_builder_test.go:70 |
| test6647 | `Test6647_analyze_path_arguments_user_input_arg_appears_in_slots` | TEST6647: Tests AnalyzePathArguments puts RequiresUserInput args in slots and sets CanExecuteWithoutInput=false Verifies that caps with non-stdin, non-default arguments are identified as requiring user input, appear in slots, and the requirements reflect that execution cannot proceed without them. | planner/plan_builder_test.go:111 |
| test6649 | `Test6649_rejects_foreach_paired_collect` | TEST6649: PlanToResolvedGraph rejects plans containing ForEach-paired Collect nodes Verifies that Collect nodes without OutputMediaUrn (ForEach-paired) are rejected | orchestrator/orchestrator_test.go:469 |
| test6660 | `Test6660_ArgumentBindingSerializationPreviousOutput` | TEST6660: ArgumentBinding PreviousOutput serializes/deserializes correctly | planner/argument_binding_test.go:301 |
| test6666 | `Test6666_StrandInputSingle` | TEST6666: StrandInput single constructor creates valid Single cardinality input | planner/argument_binding_test.go:425 |
| test6667 | `Test6667_StrandInputSequence` | TEST6667: StrandInput sequence constructor creates valid Sequence cardinality input | planner/argument_binding_test.go:440 |
| test6668 | `Test6668_CapInputFileDeserializationWithSourceMetadata` | TEST6668: CapInputFile deserializes from JSON with source metadata fields | planner/argument_binding_test.go:458 |
| test6669 | `Test6669_CapInputFileDeserializationCompact` | TEST6669: CapInputFile deserializes from compact JSON | planner/argument_binding_test.go:473 |
| test6670 | `Test6670_StrandInputInvalidSingle` | TEST6670: StrandInput validation detects mismatched Single cardinality with multiple files | planner/argument_binding_test.go:485 |
| test6700 | `Test6700_line_based_format_round_trips` | TEST6700: Line-based notation format round-trips back to the same machine. ToMachineNotationFormatted(NotationFormatLineBased) must not contain '[', and re-parsing must yield an equivalent machine. | machine/machine_test.go:746 |
| test6726 | `Test6726_cartridge_json_fabric_manifest_version_zero_round_trip` | TEST6726: CartridgeJson with FabricManifestVersion=0 (zero value) is absent on the wire. | bifaci/cartridge_json_test.go:13 |
| test6727 | `Test6727_cartridge_json_fabric_manifest_version_nonzero_round_trip` | TEST6727: CartridgeJson with FabricManifestVersion>0 round-trips the value correctly. | bifaci/cartridge_json_test.go:44 |
| test6734 | `Test6734_reject_invalid_combinations` | TEST6734: Invalid qualifier combinations must be rejected. | urn/cap_urn_test.go:1902 |
| test6735 | `Test6735_axis_weighting_out_dominates` | TEST6735: out-axis difference dominates combined in+y differences. | urn/cap_urn_test.go:1924 |
| test6736 | `Test6736_axis_weighting_decoded_layout` | TEST6736: Decoded layout — 10000*out + 100*in + y. | urn/cap_urn_test.go:1946 |
| test6745 | `Test6745_RelaySwitchNewRejectsDuplicateIDs` | TEST6745: RelaySwitch::new rejects duplicate ids in its cardinality list. | bifaci/relay_switch_test.go:1099 |
---

## Numbering Mismatches

These tests have a numbering disagreement between the function name and the authoritative immediate TEST comment/docstring above the test. This is reported explicitly so comment sync does not silently overwrite a misnumbered test.

- `test001` / `test1` / `Test001_cap_urn_creation` — urn/cap_urn_test.go:33
- `test002` / `test2` / `Test002_direction_specs_default_to_wildcard` — urn/cap_urn_test.go:55
- `test003` / `test3` / `Test003_direction_matching` — urn/cap_urn_test.go:74
- `test004` / `test4` / `Test004_unquoted_values_lowercased` — urn/cap_urn_test.go:96
- `test005` / `test5` / `Test005_quoted_values_preserve_case` — urn/cap_urn_test.go:120
- `test006` / `test6` / `Test006_quoted_value_special_chars` — urn/cap_urn_test.go:146
- `test007` / `test7` / `Test007_quoted_value_escape_sequences` — urn/cap_urn_test.go:167
- `test008` / `test8` / `Test008_mixed_quoted_unquoted` — urn/cap_urn_test.go:188
- `test009` / `test9` / `Test009_unterminated_quote_error` — urn/cap_urn_test.go:202
- `test010` / `test10` / `Test010_invalid_escape_sequence_error` — urn/cap_urn_test.go:212
- `test011` / `test11` / `Test011_serialization_smart_quoting` — urn/cap_urn_test.go:229
- `test012` / `test12` / `Test012_round_trip_simple` — urn/cap_urn_test.go:260
- `test013` / `test13` / `Test013_round_trip_quoted` — urn/cap_urn_test.go:271
- `test014` / `test14` / `Test014_round_trip_escapes` — urn/cap_urn_test.go:285
- `test015` / `test15` / `Test015_cap_prefix_required` — urn/cap_urn_test.go:299
- `test016` / `test16` / `Test016_trailing_semicolon_equivalence` — urn/cap_urn_test.go:316
- `test017` / `test17` / `Test017_tag_matching` — urn/cap_urn_test.go:352
- `test018` / `test18` / `Test018_matching_case_sensitive_values` — urn/cap_urn_test.go:382
- `test019` / `test19` / `Test019_missing_tag_handling` — urn/cap_urn_test.go:396
- `test020` / `test20` / `Test020_specificity` — urn/cap_urn_test.go:425
- `test021` / `test21` / `Test021_builder` — urn/cap_urn_test.go:454
- `test022` / `test22` / `Test022_builder_requires_direction` — urn/cap_urn_test.go:471
- `test023` / `test23` / `Test023_builder_preserves_case` — urn/cap_urn_test.go:492
- `test024` / `test24` / `Test024_directional_accepts` — urn/cap_urn_test.go:506
- `test025` / `test25` / `Test025_best_match` — urn/cap_urn_test.go:536
- `test026` / `test26` / `Test026_merge_and_subset` — urn/cap_urn_test.go:565
- `test027` / `test27` / `Test027_wildcard_tag` — urn/cap_urn_test.go:597
- `test029` / `test29` / `Test029_minimal_cap_urn` — urn/cap_urn_test.go:631
- `test030` / `test30` / `Test030_extended_character_support` — urn/cap_urn_test.go:640
- `test031` / `test31` / `Test031_wildcard_restrictions` — urn/cap_urn_test.go:655
- `test032` / `test32` / `Test032_duplicate_key_rejection` — urn/cap_urn_test.go:673
- `test033` / `test33` / `Test033_numeric_key_restriction` — urn/cap_urn_test.go:683
- `test034` / `test34` / `Test034_empty_value_error` — urn/cap_urn_test.go:709
- `test035` / `test35` / `Test035_has_tag_case_sensitive` — urn/cap_urn_test.go:720
- `test036` / `test36` / `Test036_with_tag_preserves_value` — urn/cap_urn_test.go:734
- `test037` / `test37` / `Test037_with_tag_rejects_empty_value` — urn/cap_urn_test.go:744
- `test038` / `test38` / `Test038_semantic_equivalence` — urn/cap_urn_test.go:752
- `test039` / `test39` / `Test039_get_tag_returns_direction_specs` — urn/cap_urn_test.go:761
- `test040` / `test40` / `Test040_matching_semantics_exact_match` — urn/cap_urn_test.go:785
- `test041` / `test41` / `Test041_matching_semantics_cap_missing_tag` — urn/cap_urn_test.go:796
- `test042` / `test42` / `Test042_matching_semantics_cap_has_extra_tag` — urn/cap_urn_test.go:813
- `test043` / `test43` / `Test043_matching_semantics_request_has_wildcard` — urn/cap_urn_test.go:830
- `test044` / `test44` / `Test044_matching_semantics_cap_has_wildcard` — urn/cap_urn_test.go:841
- `test045` / `test45` / `Test045_matching_semantics_value_mismatch` — urn/cap_urn_test.go:852
- `test046` / `test46` / `Test046_matching_semantics_fallback_pattern` — urn/cap_urn_test.go:863
- `test047` / `test47` / `Test047_matching_semantics_thumbnail_void_input` — urn/cap_urn_test.go:879
- `test048` / `test48` / `Test048_matching_semantics_wildcard_direction_matches_anything` — urn/cap_urn_test.go:896
- `test049` / `test49` / `Test049_matching_semantics_cross_dimension_independence` — urn/cap_urn_test.go:911
- `test050` / `test50` / `Test050_matching_semantics_direction_mismatch` — urn/cap_urn_test.go:928
- `test051` / `test51` / `Test051_input_validation_success` — cap/validation_test.go:58
- `test052` / `test52` / `Test052_input_validation_missing_required` — cap/validation_test.go:83
- `test053` / `test53` / `Test053_input_validation_wrong_type` — cap/validation_test.go:101
- `test060` / `test60` / `Test060_wrong_prefix_fails` — urn/media_urn_test.go:39
- `test062` / `test62` / `Test062_is_record` — urn/media_urn_test.go:56
- `test063` / `test63` / `Test063_is_scalar` — urn/media_urn_test.go:85
- `test064` / `test64` / `Test064_is_list` — urn/media_urn_test.go:106
- `test065` / `test65` / `Test065_is_opaque` — urn/media_urn_test.go:121
- `test066` / `test66` / `Test066_is_json` — urn/media_urn_test.go:152
- `test067` / `test67` / `Test067_is_text` — urn/media_urn_test.go:167
- `test068` / `test68` / `Test068_is_void` — urn/media_urn_test.go:192
- `test071` / `test71` / `Test071_to_string_roundtrip` — urn/media_urn_test.go:220
- `test072` / `test72` / `Test072_constants_parse` — urn/media_urn_test.go:233
- `test073` / `test73` / `Test073_extension_helpers` — urn/media_urn_test.go:273
- `test074` / `test74` / `Test074_media_urn_matching` — urn/media_urn_test.go:282
- `test075` / `test75` / `Test075_matching` — urn/media_urn_test.go:303
- `test076` / `test76` / `Test076_specificity` — urn/media_urn_test.go:315
- `test077` / `test77` / `Test077_serde_roundtrip` — urn/media_urn_test.go:327
- `test078` / `test78` / `Test078_object_does_not_conform_to_string` — urn/media_urn_test.go:344
- `test088` / `test88` / `Test088_resolve_seeded_spec` — media/spec_test.go:42
- `test089` / `test89` / `Test089_resolve_seeded_record_spec` — media/spec_test.go:56
- `test0090` / `test90` / `Test0090_absent_scan_root_yields_empty_roster` — bifaci/cartridge_discovery_test.go:65
- `test0091` / `test91` / `Test0091_missing_cartridge_json_is_manifest_invalid` — bifaci/cartridge_discovery_test.go:73
- `test0092` / `test92` / `Test0092_channel_mismatch_is_bad_installation` — bifaci/cartridge_discovery_test.go:82
- `test0094` / `test94` / `Test0094_fabric_manifest_mismatch_is_flagged` — bifaci/cartridge_discovery_test.go:93
- `test095` / `test95` / `Test095_media_def_def_serialize` — media/spec_test.go:103
- `test096` / `test96` / `Test096_media_def_def_deserialize` — media/spec_test.go:129
- `test097` / `test97` / `Test097_validate_no_duplicate_urns_catches_duplicates` — media/spec_test.go:145
- `test098` / `test98` / `Test098_validate_no_duplicate_urns_passes_for_unique` — media/spec_test.go:157
- `test0120` / `test120` / `Test0120_registry_url_under_dev_slug_is_rejected` — bifaci/cartridge_discovery_test.go:103
- `test0289` / `test289` / `Test0289_media_def_def_documentation_round_trip` — media/spec_test.go:618

---

*Generated from Go source tree*
*Total tests: 1050*
*Total numbered tests: 1050*
*Total unnumbered tests: 0*
*Total numbered tests missing descriptions: 0*
*Total numbering mismatches: 80*
