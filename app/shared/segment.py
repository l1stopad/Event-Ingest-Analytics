import re
from typing import Tuple, Dict, Any, Optional

_KEY_RE = re.compile(r"^[A-Za-z0-9_]+$")

def build_segment_filter(segment: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    """
    Повертає (sql_snippet, params) для WHERE.
    Приклади:
      - "event_type:purchase" -> "AND event_type = %(seg_event_type)s", {"seg_event_type": "purchase"}
      - "properties.country=UA" -> "AND properties ->> 'country' = %(seg_prop_val)s", {"seg_prop_val": "UA"}
    """
    if not segment:
        return "", {}

    s = segment.strip()
    # event_type:<value>
    if s.startswith("event_type:"):
        val = s.split(":", 1)[1].strip()
        return "AND event_type = %(seg_event_type)s", {"seg_event_type": val}

    # properties.<key>=<value>
    if s.startswith("properties."):
        body = s[len("properties.") :]
        if "=" not in body:
            return "", {}
        key, val = body.split("=", 1)
        key = key.strip()
        if not _KEY_RE.match(key):
            
            return "", {}
        return f"AND properties ->> '{key}' = %(seg_prop_val)s", {"seg_prop_val": val.strip()}

    
    return "", {}
