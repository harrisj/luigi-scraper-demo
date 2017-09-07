import luigi
import os
import requests
from bs4 import BeautifulSoup
import util
import csv

# https://ucr.fbi.gov/crime-in-the-u.s/2013/crime-in-the-u.s.-2013/tables/table-8/table-8-state-cuts/table_8_offenses_known_to_law_enforcement_arizona_by_city_2013.xls
# https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-8/table-8-by-state/Table_8_Offenses_Known_to_Law_Enforcement_by_Alabama_by_City_2014.xls
# https://ucr.fbi.gov/crime-in-the-u.s/2015/crime-in-the-u.s.-2015/tables/table-8/table-8-state-pieces/table_8_offenses_known_to_law_enforcement_arizona_by_city_2015.xls

DATA_DIR = 'data'

class FetchStateAgenciesPage(luigi.Task):
    year = luigi.IntParameter()
    state = luigi.Parameter()
    table = luigi.IntParameter()
    url = luigi.Parameter()

    def run(self):
        r = requests.get(self.url)
        with self.output().open('w') as out_file:
            out_file.write(r.text)

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'pages', '{0}_{1}_{2}.htm'.format(self.year, self.table, util.state_abbr(self.state))))


class ScrapeStateAgenciesPage(luigi.Task):
    year = luigi.IntParameter()
    state = luigi.Parameter()
    table = luigi.IntParameter()
    url = luigi.Parameter()

    def requires(self):
        return FetchStateAgenciesPage(year=self.year, state=self.state, table=self.table, url=self.url)

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'csv', '{0}_{1}_{2}.csv'.format(self.year, self.table, util.state_abbr(self.state))))

    def run(self):
        with self.input().open('r') as in_file:
            soup = BeautifulSoup(in_file, 'html.parser')
            # first read the footnotes
            footnotes = util.footnotes_hash(soup)
            agencies = util.agencies_with_footnotes(soup, footnotes)
            with self.output().open('wb') as csvfile:
                csvwriter = csv.writer(csvfile)
                for agency_name, footnote in agencies:
                    csvwriter.writerow([self.year, self.year, util.state_abbr(self.state), None, util.agency_type(self.table), agency_name, footnote])

class FetchAgenciesPage(luigi.Task):
    year = luigi.IntParameter()
    url = luigi.Parameter()
    table = luigi.IntParameter()

    def run(self):
        r = requests.get(self.url)
        with self.output().open('w') as out_file:
            out_file.write(r.text)

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'pages', '{0}_{1}.htm'.format(self.year, self.table)))

class ScrapeAgenciesPage(luigi.Task):
    year = luigi.IntParameter()
    table = luigi.IntParameter()
    url = luigi.Parameter()

    def requires(self):
        return FetchAgenciesPage(url=self.url, year=self.year, table=self.table)

    def run(self):
        with self.input().open('r') as in_file:
            soup = BeautifulSoup(in_file, 'html.parser')
            for link in soup.find_all('a', 'arrow-left-large'):
                yield ScrapeStateAgenciesPage(url=link.get('href'), state=link.text, year=self.year, table=self.table)

        with self.output().open('w') as out_file:
            out_file.write("DONE\n")

    def output(self):
        return luigi.LocalTarget(os.path.join(DATA_DIR, 'status', '{0}_{1}.txt'.format(self.year, self.table)))

class ScrapeAgencies(luigi.WrapperTask):
    def requires(self):
        yield ScrapeAgenciesPage(year=2010, table=8, url='https://ucr.fbi.gov/crime-in-the-u.s/2010/crime-in-the-u.s.-2010/tables/10tbl08.xls/view')
        yield ScrapeAgenciesPage(year=2010, table=9, url='https://ucr.fbi.gov/crime-in-the-u.s/2010/crime-in-the-u.s.-2010/tables/10tbl09.xls/view')
        yield ScrapeAgenciesPage(year=2010, table=10, url='https://ucr.fbi.gov/crime-in-the-u.s/2010/crime-in-the-u.s.-2010/tables/10tbl10.xls/view')
        yield ScrapeAgenciesPage(year=2010, table=11, url='https://ucr.fbi.gov/crime-in-the-u.s/2010/crime-in-the-u.s.-2010/tables/10tbl11.xls/view')

        yield ScrapeAgenciesPage(year=2011, table=8, url='https://ucr.fbi.gov/crime-in-the-u.s/2011/crime-in-the-u.s.-2011/tables/table_8_offenses_known_to_law_enforcement_by_state_by_city_2011.xls/view')
        yield ScrapeAgenciesPage(year=2011, table=9, url='https://ucr.fbi.gov/crime-in-the-u.s/2011/crime-in-the-u.s.-2011/tables/table-9/view')
        yield ScrapeAgenciesPage(year=2011, table=10, url='https://ucr.fbi.gov/crime-in-the-u.s/2011/crime-in-the-u.s.-2011/tables/table_10_offenses_known_to_law_enforcement_by_state_by_metropolitan_and_nonmetropolitan_counties_2011.xls/view')
        yield ScrapeAgenciesPage(year=2011, table=11, url='https://ucr.fbi.gov/crime-in-the-u.s/2011/crime-in-the-u.s.-2011/tables/table_11_offenses_known_to_law_enforcement_by_state_by_state_tribal_and_other_agencies_2011.xls/view')

        yield ScrapeAgenciesPage(year=2012, table=8, url='https://ucr.fbi.gov/crime-in-the-u.s/2012/crime-in-the-u.s.-2012/tables/8tabledatadecpdf/table_8_offenses_known_to_law_enforcement_by_state_by_city_2012.xls/view')
        yield ScrapeAgenciesPage(year=2012, table=9, url='https://ucr.fbi.gov/crime-in-the-u.s/2012/crime-in-the-u.s.-2012/tables/9tabledatadecpdf/table_9_offenses_known_to_law_enforcement_by_state_university_and_college_2012.xls/view')
        yield ScrapeAgenciesPage(year=2012, table=10, url='https://ucr.fbi.gov/crime-in-the-u.s/2012/crime-in-the-u.s.-2012/tables/10tabledatadecpdf/table_10_offenses_known_to_law_enforcement_by_state_by_metropolitan_and_nonmetropolitan_counties_2012.xls/view')
        yield ScrapeAgenciesPage(year=2012, table=11, url='https://ucr.fbi.gov/crime-in-the-u.s/2012/crime-in-the-u.s.-2012/tables/11tabledatadecpdf/table_11_offenses_known_to_law_enforcement_by_state_by_state_tribal_and_other_agencies_2012.xls/view')

        yield ScrapeAgenciesPage(year=2013, table=8, url='https://ucr.fbi.gov/crime-in-the-u.s/2013/crime-in-the-u.s.-2013/tables/table-8/table_8_offenses_known_to_law_enforcement_by_state_by_city_2013.xls/view')
        yield ScrapeAgenciesPage(year=2013, table=9, url='https://ucr.fbi.gov/crime-in-the-u.s/2013/crime-in-the-u.s.-2013/tables/table-9/table-9/view')
        yield ScrapeAgenciesPage(year=2013, table=10, url='https://ucr.fbi.gov/crime-in-the-u.s/2013/crime-in-the-u.s.-2013/tables/table-10/table_10_offenses_known_to_law_enforcement_by_state_by_metropolitan_and_nonmetropolitan_counties_2013.xls/view')
        yield ScrapeAgenciesPage(year=2013, table=11, url='https://ucr.fbi.gov/crime-in-the-u.s/2013/crime-in-the-u.s.-2013/tables/table-11/table_11_offenses_known_to_law_enforcement_by_state_by_state_tribal_and_other_agencies_2013.xls/view')

        yield ScrapeAgenciesPage(year=2014, table=8, url='https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-8/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2014.xls/view')
        yield ScrapeAgenciesPage(year=2014, table=9, url='https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-9/Table_9_Offenses_Known_to_Law_Enforcement_by_State_by_University_and_College_2014.xls/view')
        yield ScrapeAgenciesPage(year=2014, table=10, url='https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-10/Table_10_Offenses_Known_to_Law_Enforcement_by_State_by_Metropolitan_and_Nonmetropolitan_Counties_2014.xls/view')
        yield ScrapeAgenciesPage(year=2014, table=11, url='https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-11/Table_11_Offenses_Known_to_Law_Enforcement_by_State_by_State_Tribal_and_Other_Agencies_2014.xls/view')

        yield ScrapeAgenciesPage(year=2015, table=8, url='https://ucr.fbi.gov/crime-in-the-u.s/2015/crime-in-the-u.s.-2015/tables/table-8/table_8_offenses_known_to_law_enforcement_by_state_by_city_2015.xls/view')
        yield ScrapeAgenciesPage(year=2015, table=9, url='https://ucr.fbi.gov/crime-in-the-u.s/2015/crime-in-the-u.s.-2015/tables/table-9/table_9_offenses_known_to_law_enforcement_by_state_by_university_and_college_2015.xls/view')
        yield ScrapeAgenciesPage(year=2015, table=10, url='https://ucr.fbi.gov/crime-in-the-u.s/2015/crime-in-the-u.s.-2015/tables/table-10/table_10_offenses_known_to_law_enforcement_by_state_by_metropolitan_and_nonmetropolitan_counties_2015.xls/view')
        yield ScrapeAgenciesPage(year=2015, table=11, url='https://ucr.fbi.gov/crime-in-the-u.s/2015/crime-in-the-u.s.-2015/tables/table-11/table_11_offenses_known_to_law_enforcement_by_state_by_state_tribal_and_other_agencies_2015.xls/view')
