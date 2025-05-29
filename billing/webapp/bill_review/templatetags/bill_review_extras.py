from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """Get an item from a dictionary using the key."""
    if not dictionary or not key:
        return {}
    return dictionary.get(key, {})

@register.filter
def get_category(cpt_categories, cpt_code):
    """Get category for a CPT code."""
    if not cpt_categories or not cpt_code:
        return "Unknown"
    category_data = cpt_categories.get(cpt_code, {})
    return category_data.get('category', 'Unknown')

@register.filter
def get_subcategory(cpt_categories, cpt_code):
    """Get subcategory for a CPT code."""
    if not cpt_categories or not cpt_code:
        return ""
    category_data = cpt_categories.get(cpt_code, {})
    return category_data.get('subcategory', '')

@register.filter
def is_ancillary(cpt_code, ancillary_codes):
    """Check if a CPT code is ancillary."""
    if not cpt_code or not ancillary_codes:
        return False
    return cpt_code in ancillary_codes

@register.filter
def multiply(value, arg):
    """Multiply two values."""
    try:
        return float(value) * float(arg)
    except (ValueError, TypeError):
        return 0

@register.filter
def percentage(value, total):
    """Calculate percentage."""
    try:
        if float(total) == 0:
            return 0
        return round((float(value) / float(total)) * 100, 1)
    except (ValueError, TypeError):
        return 0 