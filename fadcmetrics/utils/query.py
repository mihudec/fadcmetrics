from typing import Literal


class ObjectQuery:

    def __init__(self, key: str, value, operator: Literal['equal', 'notEqual', 'contains', 'notContains']):
        self.key = key
        self.value = value
        self.operator = operator


class QuerySentinel:
    pass

def query_filter(objects, query: ObjectQuery):
    for obj in objects:
        if query is not None:
            actual_value = QuerySentinel()
            try:
                actual_value = getattr(obj, query.key)
            except AttributeError as e:
                raise
            if query.operator == 'Eq':
                if actual_value != query.value:
                    continue
            if query.operator == 'notEq':
                if actual_value == query.value:
                    continue
            if query.operator == 'In':
                if not isinstance(query.value, list):
                    raise ValueError("When using operator=In, value must be a list")
                if isinstance(actual_value, list):
                    if not any([sub_value in actual_value for sub_value in query.value]):
                        continue
                else:
                    if actual_value not in query.value:
                        continue
            if query.operator == 'notIn':
                if not isinstance(query.value, list):
                    raise ValueError("When using operator=In, value must be a list")
                if isinstance(actual_value, list):
                    if any([sub_value in actual_value for sub_value in query.value]):
                        continue
                else:
                    if actual_value in query.value:
                        continue
        yield obj