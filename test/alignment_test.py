from clodius.alignment import align_sequences, alignment_to_subs


def test_alignment_to_subs():
    a = align_sequences("TTTTT", "TTATT")
    s = alignment_to_subs(a)

    assert s[0] == 1
    assert s[1] == 6
    assert s[2][0]["pos"] == 2  # subs are 0-based
    assert s[2][0]["base"] == "T"
    assert s[2][0]["variant"] == "A"

    a = align_sequences("TTTTT", "TTATTT")
    s = alignment_to_subs(a)

    assert s[0] == 1
    assert s[1] == 6
    assert s[2][0]["pos"] == 2
    assert s[2][0]["type"] == "I"
    assert s[2][0]["length"] == 1
