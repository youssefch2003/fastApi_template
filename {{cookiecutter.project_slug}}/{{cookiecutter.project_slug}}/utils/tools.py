from typing import Dict, Any


def build_filters(schema_class, query, filters: Dict[str, Any]) -> Any:
    for attr, value in filters.items():
        if '__' in attr:
            relationship, field = attr.split('__', 1)
            related_class = getattr(schema_class, relationship).property.mapper.class_
            query = query.join(getattr(schema_class, relationship))
            query = query.filter(getattr(related_class, field) == value)
        else:
            query = query.filter(getattr(schema_class, attr) == value)
    return query
