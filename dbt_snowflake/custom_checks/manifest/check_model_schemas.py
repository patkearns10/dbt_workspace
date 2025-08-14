from typing import TYPE_CHECKING, Literal, Dict, List

from pydantic import Field
from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.utils import get_clean_model_name

if TYPE_CHECKING:
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_bouncer.parsers import DbtBouncerModelBase


class CheckModelSchemas(BaseCheck):
    """
    Checks model schema/name conventions based on folder location.

    Rules are supplied in `dbt-bouncer.yml` like:
      manifest_checks:
        - name: check_model_schemas
          rules:
            models/staging: ["stg_"]
            models/marts: ["fct_", "dim_"]
    """

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_schemas"]
    rules: Dict[str, List[str]] = Field(default_factory=dict)

    def execute(self) -> None:
        """Validate the model name against the configured folder->prefix rules."""
        model_name = get_clean_model_name(self.model.unique_id)
        model_path = self.model.original_file_path.replace("\\", "/").lower()

        for folder, valid_prefixes in self.rules.items():
            folder_lc = folder.lower()
            if folder_lc in model_path:
                if not any(model_name.startswith(prefix) for prefix in valid_prefixes):
                    raise AssertionError(
                        f"Model `{model_name}` in `{folder}` must start with one of: {', '.join(valid_prefixes)}"
                    )
                break  # Stop after first matching folder