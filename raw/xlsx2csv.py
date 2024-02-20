from subprocess import check_call

from clldutils.path import Path
from clldutils.dsv import UnicodeWriter


def xlsx2csv(p):
    check_call(['soffice', '--headless', '--convert-to', 'csv', str(p)])


if __name__ == '__main__':
    for p in Path('.').glob('*.xlsx'):
        xlsx2csv(p)
