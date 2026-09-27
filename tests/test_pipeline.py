from pipeline import clean_amount, clean_category, clean_date


def test_clean_date_parses_every_known_format():
    assert clean_date("03/14/2024") == ("2024-03-14", None)
    assert clean_date("2024-03-14") == ("2024-03-14", None)
    assert clean_date("14-Mar-2024") == ("2024-03-14", None)
    assert clean_date("March 14, 2024") == ("2024-03-14", None)
    assert clean_date("03-14-2024") == ("2024-03-14", None)
    assert clean_date("3/14/24") == ("2024-03-14", None)
    assert clean_date("Mar 14 2024") == ("2024-03-14", None)
    assert clean_date("Mar 14, 2024") == ("2024-03-14", None)


def test_clean_date_rejects_missing_and_unparseable():
    assert clean_date("") == (None, "missing_date")
    assert clean_date(None) == (None, "missing_date")
    assert clean_date("   ") == (None, "missing_date")
    assert clean_date("not a date") == (None, "unparseable_date")
    assert clean_date("13/45/2024") == (None, "unparseable_date")


def test_clean_category_exact_and_normalized_matches():
    assert clean_category("Travel") == ("Travel", None, False)
    assert clean_category("travel") == ("Travel", None, False)
    assert clean_category("Office_Supplies") == ("Office Supplies", None, False)
    assert clean_category("MEALS & ENTERTAINMENT") == ("Meals & Entertainment", None, False)
    # Regression check for the line-187-vs-36 normalization mismatch: the "&"
    # must be stripped the same way on both the lookup keys and the input.
    assert clean_category("Meals&Entertainment") == ("Meals & Entertainment", None, False)


def test_clean_category_fuzzy_matches_are_flagged():
    value, err, was_fuzzy = clean_category("Travle")
    assert (value, err) == ("Travel", None)
    assert was_fuzzy is True

    value, err, was_fuzzy = clean_category("Proffesional Services")
    assert (value, err) == ("Professional Services", None)
    assert was_fuzzy is True


def test_clean_category_rejects_missing_and_unrecognized():
    assert clean_category("") == (None, "missing_category", False)
    assert clean_category(None) == (None, "missing_category", False)
    assert clean_category("Xyzzy Quux") == (None, "unrecognized_category", False)


def test_clean_amount_parses_valid_numbers():
    assert clean_amount("100.00") == (100.0, None)
    assert clean_amount("$1,234.56") == (1234.56, None)
    assert clean_amount("  42  ") == (42.0, None)


def test_clean_amount_rejects_non_positive_missing_and_invalid():
    assert clean_amount("0") == (None, "non_positive_amount")
    assert clean_amount("-5.00") == (None, "non_positive_amount")
    assert clean_amount("") == (None, "missing_amount")
    assert clean_amount(None) == (None, "missing_amount")
    assert clean_amount("not a number") == (None, "invalid_amount")
