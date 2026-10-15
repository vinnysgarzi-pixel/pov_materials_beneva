from datetime import date, timedelta

import calendar_rules as cr


def matches(rule, year):
    d, out = date(year, 1, 1), []
    while d.year == year:
        if rule(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def test_friday_or_previous_working_day():
    days = matches(cr.friday_or_previous_working_day, 2026)
    assert len(days) == 53  # 2026 has 53 ISO weeks touching Jan 1-Dec 31
    # Fri 2026-01-02 is a normal Friday
    assert date(2026, 1, 2) in days
    # one match per ISO week, never on a non-working day
    assert all(cr.is_working_day(d) for d in days)
    assert len({d.isocalendar().week for d in days}) == len(days)


def test_friday_holiday_moves_to_thursday():
    # Good Friday 2026-04-03 is a Quebec statutory holiday -> Thursday 04-02
    assert not cr.is_working_day(date(2026, 4, 3))
    assert cr.friday_or_previous_working_day(date(2026, 4, 2))
    assert not cr.friday_or_previous_working_day(date(2026, 4, 3))


def test_before_last_working_day_of_week():
    assert cr.before_last_working_day_of_week(date(2026, 1, 15))  # normal week: Thursday
    assert not cr.before_last_working_day_of_week(date(2026, 1, 16))
    # Good Friday week: working days Mon-Thu -> Wednesday
    assert cr.before_last_working_day_of_week(date(2026, 4, 1))
    assert not cr.before_last_working_day_of_week(date(2026, 4, 2))


def test_third_working_day_of_quarter_start():
    days = matches(cr.third_working_day_of_quarter_start, 2026)
    assert [d.month for d in days] == [1, 4, 7, 10]
    assert days[0] == date(2026, 1, 6)  # Jan 1 holiday; Jan 2, 5, 6 -> 3rd = Jan 6
    assert days[1] == date(2026, 4, 6)  # Apr 1, 2, (3 Good Friday off), 6
    assert all(cr.is_working_day(d) for d in days)


def test_sixteenth_working_day_before_month_end():
    days = matches(cr.sixteenth_working_day_before_month_end, 2026)
    assert [d.month for d in days] == list(range(1, 13))
    # Sep 2026: 21 working days (Labour Day 09-07 off); last = Wed 09-30
    sep = cr.working_days_of_month(2026, 9)
    assert days[8] == sep[-16]
    assert len([d for d in sep if d >= days[8]]) == 16
