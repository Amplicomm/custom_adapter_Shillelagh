import urllib
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from sqlalchemy.engine.url import URL

from shillelagh.backends.apsw.dialects.base import APSWDialect
# Still work in progress ....
class APIDialect(APSWDialect):
    name = "s3"
    supports_statement_cache = True

    def create_connect_args(self, url: URL) -> Tuple[Tuple[()], Dict[str, Any]]:
        parsed = urllib.parse.urlparse(url)
        bucket = parsed.netloc
        prefix = parsed.path.strip("/") + "/"

        return (), {
            "path": ":memory:",
            "adapters": ["custom_s3select"],
            "adapter_kwargs": {
                "custom_s3select": {
                    "bucket": bucket,
                    "prefix": prefix,
                },
            },
            "safe": True,
            "isolation_level": self.isolation_level,
        }

