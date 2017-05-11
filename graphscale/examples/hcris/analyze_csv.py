import sys
import csv
import re

class OnePassFilterRule:
    def __init__(self, filter_fn, description):
        self.passes = False
        self.filter_fn = filter_fn
        self._description = description

    def process(self, value):
        if not self.passes:
            self.passes = self.filter_fn(value)

    @property
    def description(self):
        return self._description

class AllPassFilterRule:
    def __init__(self, filter_fn, description):
        self.passes = True
        self.filter_fn = filter_fn
        self._description = description

    def process(self, value):
        if self.passes:
            self.passes = self.filter_fn(value)

    @property
    def description(self):
        return self._description

class FixedLengthRule:
    def __init__(self):
        self.passes = True
        self.length = None

    def process(self, value):
        if self.length is None:
            self.length = len(value)
        if self.passes:
            self.passes = len(value) == self.length

    @property
    def description(self):
        return 'Fixed Length: ' + str(self.length)

class DefinedSetRule:
    def __init__(self):
        self.passes = True
        self.values = set()

    def process(self, value):
        if self.passes:
            self.values.add(value)
            if len(self.values) > 100:
                self.passes = False
                self.values = None

    @property
    def description(self):
        return 'Defined Set: ' + str(sorted(list(self.values)))

class RegexRule:
    def __init__(self, pattern, description):
        self.passes = True
        self.compiled = re.compile(pattern)
        self._description = description

    def process(self, value):
        if not value:
            return
        if self.passes:
            if not self.compiled.match(value):
                self.passes = False

    @property
    def description(self):
        return self._description

def is_int(value, base=10, val=None):
    try:
        return int(value, base)
    except ValueError:
        return val

def int_rule():
    return AllPassFilterRule(is_int, 'Int')

def has_nulls_rule():
    return OnePassFilterRule(lambda value: not value, 'Has Nulls')

def all_nulls_rules():
    return AllPassFilterRule(lambda value: not value, 'All Nulls')

def is_weird_alpha_date(value):
    parts = value.split('-')
    if len(parts) != 3:
        return False

    day, month, year = tuple(parts)

    if not (is_int(day) and int(day) > 0 and int(day) <= 31):
        return False

    months = set(['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                  'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'])
    if not month in months:
        return False

    return is_int(year) and int(year) > 0 and int(year) <= 99


class ColumnTracker:
    def __init__(self, name):
        self.name = name
        self.defined_set = True
        self.values = set()

        self.rules = [
            has_nulls_rule(),
            all_nulls_rules(),
            int_rule(),
            FixedLengthRule(),
            DefinedSetRule(),
            AllPassFilterRule(is_weird_alpha_date, 'Weird Alpha Data e.g. 01-OCT-15'),
            RegexRule(r'[A-Z]+$', 'All Uppercase Characters'),
            RegexRule(r'\d{5}(-(\d{4})?)?$', 'Messy Zips: XXXXX, XXXXX- or XXXXX-XXXX'),
            RegexRule(r'\d{1,2}/\d{1,2}/\d{4}$',
                      'American Style Dates: e.g. 1/2/2010 or 10/12/2034'),
        ]

    def process_value(self, value):
        for rule in self.rules:
            rule.process(value)

    def rules_results(self):
        results = []
        for rule in self.rules:
            if rule.passes:
                results.append(rule.description)
        return results

def do_analysis(args):
    path = args[1]
    trackers = {}
    num_rows = 0
    with open(path, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        column_names = next(row_reader)
        trackers = {column_name: ColumnTracker(column_name) for column_name in column_names}
        for data_row in row_reader:
            num_rows += 1
            data = dict(zip(column_names, data_row))
            for key, value in data.items():
                trackers[key].process_value(value)

    print('Rows: ' + str(num_rows))
    for name, tracker in trackers.items():
        print('Column: ' + name)
        for description in tracker.rules_results():
            print('    ' + description)

        print('')

if __name__ == '__main__':
    do_analysis(sys.argv)
