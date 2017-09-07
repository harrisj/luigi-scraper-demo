import util
from bs4 import BeautifulSoup

class TestFootnoteScraping:
    def test_footnotes_hash_create(self):
        body = '<ul class="tablenotes">\
<li><sup>1</sup> The figures shown in this column for the offense of rape were reported using the revised Uniform Crime Reporting (UCR) definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>2</sup> The figures shown in this column for the offense of rape were reported using the legacy UCR definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>3</sup> Because of changes in the state/local agency''s reporting practices, figures are not comparable to previous years'' data.</li>\
</ul>'
        soup = BeautifulSoup(body, 'html.parser')
        footnotes = util.footnotes_hash(soup)

        assert len(footnotes) == 3
        assert sorted(footnotes.keys()) == [1, 2, 3]
        assert footnotes[2] == 'The figures shown in this column for the offense of rape were reported using the legacy UCR definition of rape. See Data Declaration for further explanation.'

    def test_footnotes_hash_empty(self):
        soup = BeautifulSoup('', 'html.parser')
        footnotes = util.footnotes_hash(soup)
        assert len(footnotes) == 0

    def test_agencies_with_no_footnotes(self):
        body = '<table class="data"><tbody><tr><th>West Covina</tr></tbody></table><ul class="tablenotes">\
<li><sup>1</sup> The figures shown in this column for the offense of rape were reported using the revised Uniform Crime Reporting (UCR) definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>2</sup> The figures shown in this column for the offense of rape were reported using the legacy UCR definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>3</sup> Because of changes in the state/local agency''s reporting practices, figures are not comparable to previous years'' data.</li>\
</ul>'
        soup = BeautifulSoup(body, 'html.parser')
        footnotes_hash = util.footnotes_hash(soup)
        agencies = util.agencies_with_footnotes(soup, footnotes_hash)
        assert len(agencies) == 0

    def test_agencies_with_footnotes(self):
        body = '<table class="data"><tbody><tr><th>West Covina<sup>2</sup></tr></tbody></table><ul class="tablenotes">\
<li><sup>1</sup> The figures shown in this column for the offense of rape were reported using the revised Uniform Crime Reporting (UCR) definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>2</sup> The figures shown in this column for the offense of rape were reported using the legacy UCR definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>3</sup> Because of changes in the state/local agency''s reporting practices, figures are not comparable to previous years'' data.</li>\
</ul>'
        soup = BeautifulSoup(body, 'html.parser')
        footnotes_hash = util.footnotes_hash(soup)
        agencies = util.agencies_with_footnotes(soup, footnotes_hash)
        assert len(agencies) == 1
        assert agencies[0] == ['West Covina', 'The figures shown in this column for the offense of rape were reported using the legacy UCR definition of rape. See Data Declaration for further explanation.']

    def test_agencies_with_multiple_footnotes(self):
        body = '<table class="data"><tbody><tr><th>West Covina<sup>1, 2</sup></tr></tbody></table><ul class="tablenotes">\
<li><sup>1</sup> The figures shown in this column for the offense of rape were reported using the revised Uniform Crime Reporting (UCR) definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>2</sup> The figures shown in this column for the offense of rape were reported using the legacy UCR definition of rape. See Data Declaration for further explanation.</li>\
<li><sup>3</sup> Because of changes in the state/local agency''s reporting practices, figures are not comparable to previous years'' data.</li>\
</ul>'
        soup = BeautifulSoup(body, 'html.parser')
        footnotes_hash = util.footnotes_hash(soup)
        agencies = util.agencies_with_footnotes(soup, footnotes_hash)
        assert len(agencies) == 1
        assert agencies[0] == ['West Covina', 'The figures shown in this column for the offense of rape were reported using the revised Uniform Crime Reporting (UCR) definition of rape. See Data Declaration for further explanation. The figures shown in this column for the offense of rape were reported using the legacy UCR definition of rape. See Data Declaration for further explanation.']

    def test_agencies_with_bad_footnotes(self):
        body = '<table class="data"><tbody><tr><th>West Covina<sup>2</sup></tr></tbody></table><ul class="tablenotes">\
<li><sup>1</sup> The figures shown in this column for the offense of rape were reported using the revised Uniform Crime Reporting (UCR) definition of rape. See Data Declaration for further explanation.</li>\
</ul>'
        soup = BeautifulSoup(body, 'html.parser')
        footnotes_hash = util.footnotes_hash(soup)
        agencies = util.agencies_with_footnotes(soup, footnotes_hash)
        assert len(agencies) == 1
        assert agencies[0] == ['West Covina', '[BAD FOOTNOTE]']
