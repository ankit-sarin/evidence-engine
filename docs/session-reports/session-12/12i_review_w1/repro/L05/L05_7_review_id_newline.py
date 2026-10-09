"""The requested-id check uses re.match with `$`, which also matches before a trailing newline."""
from engine.core.review_paths import data_root_for, spec_path_for
print(repr(data_root_for("surgical_autonomy\n")), repr(spec_path_for("surgical_autonomy\n")))
from engine.core.review_spec import ReviewSpec
import pydantic
try:
    ReviewSpec.__pydantic_validator__.validate_assignment(ReviewSpec.model_construct(), "review_id", "surgical_autonomy\n")
    print("spec field accepts the same string")
except pydantic.ValidationError as e:
    print("spec field refuses the same string:", e.errors()[0]["type"])
