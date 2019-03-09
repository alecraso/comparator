"""
    Comparison callables
"""
BASIC_COMP = 'basic'
LEN_COMP = 'len'
FIRST_COMP = 'first'
DEFAULT_COMP = BASIC_COMP


def basic_comp(left, right):
    return left == right


def len_comp(left, right):
    return basic_comp(len(left), len(right))


def first_eq_comp(left, right):
    return basic_comp(left.first(), right.first())


COMPS = {
    BASIC_COMP: basic_comp,
    LEN_COMP: len_comp,
    FIRST_COMP: first_eq_comp,
}
