def element_text(element):
    value = element.text
    if value:
        value.strip()
    return value


def element_to_obj(element, map_class=dict, value_f=element_text, wrap_value=True):
    value = None

    if len(element) > 0:
        child_values = map(lambda e: (e.tag, element_to_obj(
            e, map_class=map_class, value_f=value_f, wrap_value=False)), element)
        value = map_class(child_values)
    else:
        value = value_f(element)

    if wrap_value:
        value = {element.tag: value}
    return value
