"""
    Comparison callables
"""
BASIC_COMP = 'basic'
LEN_COMP = 'len'
FIRST_COMP = 'first'
DEFAULT_COMP = FIRST_COMP


def basic_comp(left, right):
    return left == right


def len_comp(left, right):
    return basic_comp(len(left), len(right))


def first_eq_comp(left, right):
    return left[0] == right[0]


COMPS = {
    BASIC_COMP: basic_comp,
    LEN_COMP: len_comp,
    FIRST_COMP: first_eq_comp,
}
