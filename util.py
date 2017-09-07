import us

def state_abbr(name):
    return us.states.lookup(name).abbr

def agency_type(table):
    types = {
        8: 'city',
        9: 'university',
        10: 'county',
        11: 'other'
    }
    return types[table]

def footnotes_hash(soup):
    footnotes = {}
    notes_li = soup.select('ul.tablenotes li')
    for li in notes_li:
        number = li.find('sup').text
        footnotes[int(number)] = li.text[len(number):].strip()

    return footnotes

def agencies_with_footnotes(soup, footnotes_hash):
    agencies = []

    agency_cells = soup.select('table.data tbody tr th')
    for acell in agency_cells:
        sup = acell.find('sup')
        if sup:
            numbers = sup.string.split(',')
            agency_name = acell.text[0:-(len(sup.string))].strip()
            agencies.append([agency_name, ' '.join([footnotes_hash.get(int(key),'[BAD FOOTNOTE]') for key in numbers])])
    return agencies
