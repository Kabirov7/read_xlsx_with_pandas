import pandas as pd
import re
import config
import psycopg2

DB = psycopg2.connect(options=f'-c search_path={config.options}', database=config.database, user=config.user,
                      password=config.password, host=config.host, port=config.port)
MY_CURSOR = DB.cursor()

only_dight = re.compile('[^0-9,]')
only_str = re.compile('[^а-яА-яa-zA-Z.% -]')


def get_name(df, forms, column):
    names = []
    for i in range(len(forms)):
        name = str(df.iloc[forms[i] + 2, column]).strip()
        names.append(name)
    return names


def get_inn(df, forms, column):
    inns = []
    for i in range(len(forms)):
        inn = str(df.iloc[forms[i] + 1, column]).replace(' ', '')
        inns.append(inn)
    return inns


def get_tax_org(df, forms, column):
    taxes_org = []
    for i in range(len(forms)):
        taxes_org.append(str(df.iloc[forms[i] + 1, column]).replace('\xa0',
                                                                    ' ').strip())  # politic-2017 sheet:политические должности
    return taxes_org


def get_work_place(df, forms, column):
    work_places = []
    for i in range(len(forms)):
        work_places.append(str(((df.iloc[forms[i] + 1, column]).strip()).replace('\n', '').replace('\xa0',
                                                                                                   ' ')))  # politic-2017 sheet:политические должности
    return work_places


def get_work_title(df, forms, column):
    work_titles = []
    for i in range(len(forms)):
        work_titles.append(str(df.iloc[forms[i] + 2, column]).strip())
    return work_titles


def get_incomes(df, individuals, column):
    incomes = []
    for i in range(len(individuals)):
        income = str(df.iloc[individuals[i] + 4, column]).strip()
        if (income == 'nan') or (income == '') or (income == '-'):
            d = 0
        elif income != 'nan':
            d = only_dight.sub('', income.replace('.', ','))
            d = float(d.replace(',', '.'))
        incomes.append(d)
    return incomes


def get_spendings(df, individuals, column):
    spendings = []
    for i in range(len(individuals)):
        spend = str(df.iloc[individuals[i] + 4, column]).strip()
        if (spend == 'nan') or (spend == '') or (spend == '-') or (spend == ' ') or (
                only_dight.sub('', spend).replace('\xa0', '').replace(',', '.').replace(' ', '') == ''):
            d = float(0)
        elif spend != 'nan':
            d = only_dight.sub('', spend.replace('.', ','))
            d = float(d.replace(',', '.'))
        spendings.append(d)
    return spendings


def get_rel_incomes(df, close_relatives, column):
    rel_incomes = []
    for i in range(len(close_relatives)):
        rel_income = str(df.iloc[close_relatives[i] + 1, column]).strip()
        if (rel_income == 'nan') or (rel_income == '-') or (rel_income == ''):
            rel_income = float(0)
        elif rel_income != 'nan':
            rel_income = only_dight.sub('', rel_income.replace('.', ','))
            rel_income = float(rel_income.replace(',', '.'))
        rel_incomes.append(rel_income)
    return rel_incomes


def get_rel_spendings(df, close_relatives, column):
    rel_spendings = []
    for i in range(len(close_relatives)):
        rel_spending = str(df.iloc[close_relatives[i] + 1, column]).strip()
        if (rel_spending == 'nan') or (rel_spending == '-') or (rel_spending == '') or (
                only_dight.sub('', rel_spending).replace('\xa0', '').replace(',', '.').replace(' ', '') == ''):
            rel_spending = float(0)
        elif rel_spending != 'nan':
            rel_spending = only_dight.sub('', rel_spending.replace('.', ','))
            rel_spending = float(rel_spending.replace(',', '.'))
        rel_spendings.append(rel_spending)
    return rel_spendings


def real_estate(df, names, inns, taxes_org, work_places, work_titles, incomes, spendings, rel_incomes, rel_spendings,
                depend, forms, individuals, close_relatives):
    persons = []
    person_data = []
    rel_data = []
    for i in range(len(forms)):
        items = close_relatives[i] - (individuals[i] + 5)
        try:
            items_relatives = (forms[i + 1] - 3) - (close_relatives[i] + 1)
        except:
            items_relatives = df.index[-1] - (close_relatives[i] + 1)

        person = {
            'name': names[i],
            'inn': int(only_dight.sub('', inns[i])),
            'taxes_org': taxes_org[i],
            'work_place': work_places[i],
            'work_title': work_titles[i],
            'income': incomes[i],
            'spendings': spendings[i],
            'rel_income': rel_incomes[i],
            'rel_spendings': rel_spendings[i],
        }

        persons.append(person)
        for item in range(0, items + 1):
            income = str(df.iloc[individuals[i] + 4 + item, depend['income']]).strip().replace('\n', ' ')
            income_unit = only_str.sub('', income)
            income = only_dight.sub('', income.replace('.', ','))
            spending = str(df.iloc[individuals[i] + 4 + item, depend['spendig']]).strip().replace('\n', ' ')
            spending_unit = only_str.sub('', spending)
            spending = only_dight.sub('', spending.replace('.', ','))
            real_estate = str(df.iloc[individuals[i] + 4 + item, depend['real_estate']]).strip().replace('\n', ' ')
            real_estate_sqr = str(df.iloc[individuals[i] + 4 + item, depend['real_estate_sqr']])
            real_estate_sqr_unit = only_str.sub('', real_estate_sqr).replace('.', ',')
            real_estate_sqr = only_dight.sub('', real_estate_sqr.replace('.', ','))
            business_entity = str(df.iloc[individuals[i] + 4 + item, depend['business_entity']]).replace('\n',
                                                                                                         ' ').replace(
                '\xa0', '').replace(',', '.').replace(' ', '')
            business_entity_share = str(df.iloc[individuals[i] + 4 + item, depend['business_entity_share']]).strip()
            personal_estate_names = str(
                df.iloc[individuals[i] + 4 + item, depend['personal_estate_names']]).strip().replace('\n', ' ')
            personal_estate_info = str(
                df.iloc[individuals[i] + 4 + item, depend['personal_estate_info']]).strip().replace('\n', ' ')
            business_entity_personal_estate = str(
                df.iloc[individuals[i] + 4 + item, depend['business_entity_personal_estate']]).strip().replace('\n',
                                                                                                               ' ')
            business_entity_share_personal_estate = str(
                df.iloc[individuals[i] + 4 + item, depend['business_entity_share_personal_estate']]).strip()
            business_entity_share_personal_estate_type = only_str.sub('', business_entity_share_personal_estate)
            business_entity_share_personal_estate = only_dight.sub('', business_entity_share_personal_estate).replace(
                ' ', ',')

            result_income = income in inns

            if result_income == True:
                income = 'nan'

            if (income == 'nan') or (len(income) == 0):
                income = float(0)

            elif income in inns == True:
                income = float(0)

            elif income != 'nan':
                if income[-1] != ',':
                    while (income[-1] == '.') or (income[-1] == ','):
                        income = income[:-1]
                elif (income[-1] == ',') and (income == ',' or income == '.'):
                    income = float(0)

                if income != float(0):
                    income = float(income.replace('\xa0', '').replace(',', '.').replace(' ', '').replace('О', '0'))

            if (income_unit == 'nan') or (income_unit == '.') or (income_unit == ',') or (income_unit == ''):
                income_unit = '-'

            if (spending == 'nan') or (len(spending) == 0) or (spending == '0'):
                spending = float(0)

            elif spending != 'nan':
                if spending[-1] == ',':
                    while (spending[-1] == '.') or (spending[-1] == ','):
                        spending = spending[:-1]
                elif (spending[-1] == ',') and (spending == ',' or spending == '.'):
                    spending = float(0)

                if spending != float(0):
                    spending = float(spending.replace('\xa0', '').replace(',', '.').replace(' ', '').replace('О', '0'))

            if (spending_unit == 'nan') or (spending_unit == '.') or (spending_unit == ',') or (spending_unit == ''):
                spending_unit = '-'

            if real_estate == 'nan' or real_estate == '0':
                real_estate = '-'

            if (real_estate_sqr == 'nan') or (len(real_estate_sqr) == 0):
                real_estate_sqr = float(0)

            elif real_estate_sqr != 'nan':
                if len(real_estate_sqr) != 1:
                    while (real_estate_sqr[-1] == '.') or (real_estate_sqr[-1] == ','):
                        real_estate_sqr = real_estate_sqr[:-1]
                elif (len(real_estate_sqr) == 1) or (spending == '.') or (spending == ','):
                    real_estate_sqr = float(0)

                if real_estate_sqr != float(0):
                    real_estate_sqr = float(real_estate_sqr.replace('\xa0', '').replace(',', '.').replace(' ', ''))

            if (real_estate_sqr_unit == 'nan') or (real_estate_sqr_unit == '.') or (real_estate_sqr_unit == ''):
                real_estate_sqr_unit = '-'

            if business_entity_share == 'nan' or business_entity_share == '0':
                business_entity_share = 0

            if business_entity == 'nan' or business_entity == '0':
                business_entity = 0

            if personal_estate_names == 'nan' or personal_estate_names == '-' or personal_estate_names == '' or personal_estate_names == '0':
                personal_estate_names = '-'

            if personal_estate_info == 'nan':
                personal_estate_info = 0

            if business_entity_personal_estate == 'nan':
                business_entity_personal_estate = 0

            if (business_entity_share_personal_estate == 'nan') or (business_entity_share_personal_estate == '') or (
                    business_entity_share_personal_estate == '-'):
                business_entity_share_personal_estate = 0

            elif business_entity_share_personal_estate != 'nan' and business_entity_share_personal_estate != '0':
                business_entity_share_personal_estate = float(
                    business_entity_share_personal_estate.replace('\xa0', '').replace(',', '.').replace(' ', ''))

            if business_entity_share_personal_estate_type == 'nan' or business_entity_share_personal_estate_type == '':
                business_entity_share_personal_estate_type = '-'

            personal_data = {
                'owner_id': item,
                'inn': int(only_dight.sub('', inns[i])),
                'income': income,
                'income_type': income_unit,
                'spending': spending,
                'spending_type': spending_unit,
                'real_estate': real_estate,
                'real_estate_sqr': real_estate_sqr,
                'real_estate_sqr_type': real_estate_sqr_unit,
                'business_entity': business_entity,
                'business_entity_share': business_entity_share,
                'personal_estate_names': personal_estate_names,
                'personal_estate_info': personal_estate_info,
                'business_entity_personal_estate': business_entity_personal_estate,
                'business_entity_share_personal_estate': business_entity_share_personal_estate,
                'business_entity_share_personal_estate_type': business_entity_share_personal_estate_type,
            }
            if (personal_data['income'] != float(0)) or (
                    personal_data['income_type'] != '-' or personal_data['income_type'] != '') or (
                    personal_data['spending'] != float(0)) or (
                    personal_data['spending_type'] != '-' or personal_data['spending_type'] != '') or (
                    personal_data['real_estate'] != '0' or personal_data['real_estate'] != '-') or (
                    personal_data['real_estate_sqr'] != float(0)) or (
                    personal_data['real_estate_sqr_type'] != '' or personal_data['real_estate_sqr_type'] != '-') or (
                    personal_data['business_entity'] != 0) or (
                    personal_data['business_entity_share'] != 0) or (personal_data['personal_estate_names'] != 0) or (
                    personal_data['personal_estate_info'] != 0) or (
                    personal_data['business_entity_personal_estate'] != 0) or (
                    personal_data['business_entity_share_personal_estate'] != 0) or (
                    personal_data['business_entity_share_personal_estate_type'] != '-'):
                person_data.append(personal_data)
        for item in range(1, items_relatives + 2):
            income = str(df.iloc[close_relatives[i] + item, depend['income']]).strip().replace('\n', ' ')
            income_unit = only_str.sub('', income)
            income = only_dight.sub('', income.replace('.', ','))
            spending = str(df.iloc[close_relatives[i] + item, depend['spendig']]).strip().replace('\n', ' ')
            spending_unit = only_str.sub('', spending)
            spending = only_dight.sub('', spending.replace('.', ','))
            real_estate = str(df.iloc[close_relatives[i] + item, depend['real_estate']]).strip().replace('\n', ' ')
            real_estate_sqr = str(df.iloc[close_relatives[i] + item, depend['real_estate_sqr']]).replace('.', ',')
            real_estate_sqr_unit = only_str.sub('', real_estate_sqr).replace('.', ',')
            real_estate_sqr = only_dight.sub('', real_estate_sqr.replace('.', ','))
            business_entity = str(df.iloc[close_relatives[i] + item, depend['business_entity']]).strip().replace('\n',
                                                                                                                 ' ')
            business_entity_share = str(df.iloc[close_relatives[i] + item, depend['business_entity_share']]).strip()
            personal_estate_names = str(
                df.iloc[close_relatives[i] + item, depend['personal_estate_names']]).strip().replace('\n', ' ')
            personal_estate_info = str(
                df.iloc[close_relatives[i] + item, depend['personal_estate_info']]).strip().replace('\n', ' ').replace(
                '\xa0', ' ')
            business_entity_personal_estate = str(
                df.iloc[close_relatives[i] + item, depend['business_entity_personal_estate']]).strip().replace('\n',
                                                                                                               ' ')
            business_entity_share_personal_estate = str(
                df.iloc[close_relatives[i] + item, depend['business_entity_share_personal_estate']]).strip()
            business_entity_share_personal_estate_type = only_str.sub('', business_entity_share_personal_estate)
            business_entity_share_personal_estate = only_dight.sub('', business_entity_share_personal_estate).replace(
                ' ', ',')

            result_income = income in inns

            if result_income == True:
                income = 'nan'

            if (income == 'nan') or (len(income) == 0):
                income = float(0)

            elif income in inns == True:
                income = float(0)

            elif income != 'nan':
                if (income[-1] != ',' or '.') and (len(income) != 1):
                    while (income[-1] == '.') or (income[-1] == ','):
                        income = income[:-1]
                elif (income[-1] == ',') and (income == ',' or income == '.'):
                    income = float(0)

                if income != float(0):
                    income = float(income.replace('\xa0', '').replace(',', '.').replace(' ', '').replace('О', '0'))

            if (income_unit == 'nan') or (income_unit == '.') or (income_unit == ',') or (income_unit == ''):
                income_unit = '-'

            if (spending == 'nan') or (len(spending) == 0) or (spending == '0'):
                spending = float(0)

            elif spending != 'nan':
                if spending[-1] == ',':
                    while (spending[-1] == '.') or (spending[-1] == ','):
                        spending = spending[:-1]
                elif (spending[-1] == ',') and (spending == ',' or spending == '.'):
                    spending = float(0)

                if spending != float(0):
                    spending = float(spending.replace('\xa0', '').replace(',', '.').replace(' ', '').replace('О', '0'))

            if (spending_unit == 'nan') or (spending_unit == '.') or (spending_unit == ',') or (spending_unit == ''):
                spending_unit = '-'

            if real_estate == 'nan' or real_estate == '0':
                real_estate = '-'

            if (real_estate_sqr == 'nan') or (len(real_estate_sqr) == 0):
                real_estate_sqr = float(0)

            elif real_estate_sqr != 'nan':
                if len(real_estate_sqr) != 1:
                    while (real_estate_sqr[-1] == '.') or (real_estate_sqr[-1] == ','):
                        real_estate_sqr = real_estate_sqr[:-1]
                elif (len(real_estate_sqr) == 1) or (real_estate_sqr == '.') or (real_estate_sqr == ','):
                    real_estate_sqr = float(0)

                if real_estate_sqr != float(0):
                    real_estate_sqr = float(real_estate_sqr.replace('\xa0', '').replace(',', '.').replace(' ', ''))

            if (real_estate_sqr_unit == 'nan') or (real_estate_sqr_unit == '.') or (real_estate_sqr_unit == ''):
                real_estate_sqr_unit = '-'

            if business_entity_share == 'nan' or business_entity_share == '0':
                business_entity_share = 0

            if business_entity == 'nan' or business_entity == '0':
                business_entity = 0

            if personal_estate_names == 'nan' or personal_estate_names == '-' or personal_estate_names == '' or personal_estate_names == '0':
                personal_estate_names = '-'

            if personal_estate_info == 'nan' or personal_estate_info == '0':
                personal_estate_info = 0

            if business_entity_personal_estate == 'nan' or business_entity_personal_estate == '0':
                business_entity_personal_estate = 0

            if (business_entity_share_personal_estate == 'nan') or (business_entity_share_personal_estate == '') or (
                    business_entity_share_personal_estate == '0') or (
                    business_entity_share_personal_estate == '-'):
                business_entity_share_personal_estate = 0

            elif business_entity_share_personal_estate != 'nan' and business_entity_share_personal_estate != '0':
                business_entity_share_personal_estate = float(
                    business_entity_share_personal_estate.replace('\xa0', '').replace(',', '.').replace(' ', ''))

            if business_entity_share_personal_estate_type == 'nan' or business_entity_share_personal_estate_type == '' or business_entity_share_personal_estate_type == '0':
                business_entity_share_personal_estate_type = '-'

            relatives_data = {
                'owner_id': item - 1,
                'inn': int(only_dight.sub('', inns[i])),
                'income': income,
                'income_type': income_unit,
                'spending': spending,
                'spending_type': spending_unit,
                'real_estate': real_estate,
                'real_estate_sqr': real_estate_sqr,
                'real_estate_sqr_type': real_estate_sqr_unit,
                'business_entity': business_entity,
                'business_entity_share': business_entity_share,
                'personal_estate_names': personal_estate_names,
                'personal_estate_info': personal_estate_info,
                'business_entity_personal_estate': business_entity_personal_estate,
                'business_entity_share_personal_estate': business_entity_share_personal_estate,
                'business_entity_share_personal_estate_type': business_entity_share_personal_estate_type,
            }

            if (relatives_data['income'] != float(0)) or (
                    relatives_data['income_type'] != '-' or relatives_data['income_type'] != '') or (
                    relatives_data['spending'] != float(0)) or (
                    relatives_data['spending_type'] != '-' or relatives_data['spending_type'] != '') or (
                    relatives_data['real_estate'] != '0' or relatives_data['real_estate'] != '-') or (
                    relatives_data['real_estate_sqr'] != float(0)) or (
                    relatives_data['real_estate_sqr_type'] != '' or relatives_data['real_estate_sqr_type'] != '-') or (
                    relatives_data['business_entity'] != 0) or (
                    relatives_data['business_entity_share'] != 0) or (relatives_data['personal_estate_names'] != 0) or (
                    relatives_data['personal_estate_info'] != 0) or (
                    relatives_data['business_entity_personal_estate'] != 0) or (
                    relatives_data['business_entity_share_personal_estate'] != 0) or (
                    relatives_data['business_entity_share_personal_estate_type'] != '-'):
                rel_data.append(relatives_data)
    return persons, person_data, rel_data


def save_person(persons, year):
    for person in persons:
        sql_formula = f'insert into person(name_, inn, year_, taxes_org, work_place, work_title, income, spending, rel_income, rel_spending) values ' \
                      f'(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (inn) do nothing'

        content = ([person['name'], person['inn'], year, person['taxes_org'], person['work_place'], person['work_title'], person['income'], person['spendings'], person['rel_income'], person['rel_spendings']])

        MY_CURSOR.execute(sql_formula, content)
        DB.commit()


def save_person_data(person_data):
    for data in person_data:
        if data['income'] != 0.0 or data['income_type'] != '-' or data['spending'] != 0.0 or data[
            'spending_type'] != '-' or data['real_estate'] != '-' or data['real_estate_sqr'] != 0.0 or data[
            'real_estate_sqr_type'] != '-' or data['business_entity'] != 0 or data['business_entity_share'] != 0 or \
                data['personal_estate_names'] != '-' or data['personal_estate_info'] != 0 or data[
            'business_entity_personal_estate'] != 0 or data['business_entity_share_personal_estate'] != 0 or data[
            'business_entity_share_personal_estate_type'] != '-':
            result_income_type = data['income_type'] in config.isnot
            result_real_estate = data['real_estate'] in config.isnot
            result_real_estate_sqr_type = data['real_estate_sqr_type'] in config.isnot
            result_business_entity = data['business_entity'] in config.isnot
            if result_income_type == False and result_real_estate == False and result_real_estate_sqr_type == False and result_business_entity == False:
                sql_formula = f'insert into person_data( owner_id, per_inn, income, income_type, spending, spending_type, ' \
                              f'real_estate, real_estate_sqr, real_estate_sqr_type, business_entity, business_entity_share, ' \
                              f'personal_estate_names, personal_estate_info, business_entity_personal_estate, ' \
                              f'business_entity_share_personal_estate, business_entity_share_personal_estate_type ) ' \
                              f'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing'

                content = (
                    [data['owner_id'], data['inn'], data['income'], data['income_type'], data['spending'],
                     data['spending_type'], data['real_estate'], data['real_estate_sqr'], data['real_estate_sqr_type'],
                     data['business_entity'], data['business_entity_share'],
                     data['personal_estate_names'], data['personal_estate_info'],
                     data['business_entity_personal_estate'],
                     data['business_entity_share_personal_estate'], data['business_entity_share_personal_estate_type']])

                MY_CURSOR.execute(sql_formula, content)
                DB.commit()


def save_rel_data(rel_data):
    for data in rel_data:
        if data['income'] != 0.0 or data['income_type'] != '-' or data['spending'] != 0.0 or data[
            'spending_type'] != '-' or data['real_estate'] != '-' or data['real_estate_sqr'] != 0.0 or data[
            'real_estate_sqr_type'] != '-' or data['business_entity'] != 0 or data['business_entity_share'] != 0 or \
                data['personal_estate_names'] != '-' or data['personal_estate_info'] != 0 or data[
            'business_entity_personal_estate'] != 0 or data['business_entity_share_personal_estate'] != 0 or data[
            'business_entity_share_personal_estate_type'] != '-':
            result_income_type = data['income_type'] in config.isnot
            result_real_estate = data['real_estate'] in config.isnot
            result_real_estate_sqr_type = data['real_estate_sqr_type'] in config.isnot
            result_business_entity = data['business_entity'] in config.isnot
            if result_income_type == False and result_real_estate == False and result_real_estate_sqr_type == False and result_business_entity == False:
                sql_formula = f'insert into rel_data( rel_owner_id, rel_inn, rel_income, rel_income_type, rel_spending, rel_spending_type, ' \
                              f'rel_real_estate, rel_real_estate_sqr, rel_real_estate_sqr_type, rel_business_entity, rel_business_entity_share, ' \
                              f'rel_personal_estate_names, rel_personal_estate_info, rel_business_entity_personal_estate, ' \
                              f'rel_business_entity_share_personal_estate, rel_business_entity_share_personal_estate_type ) ' \
                              f'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing'

                content = (
                    [data['owner_id'], data['inn'], data['income'], data['income_type'], data['spending'],
                     data['spending_type'], data['real_estate'], data['real_estate_sqr'], data['real_estate_sqr_type'],
                     data['business_entity'], data['business_entity_share'],
                     data['personal_estate_names'], data['personal_estate_info'],
                     data['business_entity_personal_estate'],
                     data['business_entity_share_personal_estate'], data['business_entity_share_personal_estate_type']])

                MY_CURSOR.execute(sql_formula, content)
                DB.commit()


def main():
    for depend in config.dependencies:
        print('file ', depend['title'])
        for sheet in range(depend['sheets']):
            print('sheet is ', sheet)

            df = pd.read_excel(depend["title"], header=None, sheet_name=sheet)

            forms = [index for index, row in
                     enumerate(df[depend['inn102']].str.contains('Идентификационный налоговый номер')) if row == True]
            individuals = [index for index, row in enumerate(df[depend['inn102']].str.contains('ДОХОДЫ')) if
                           row == True]
            close_relatives = [index for index, row in enumerate(df[depend['inn102']].str.contains('РАЗДЕЛ II.')) if
                               row == True]

            names = get_name(df, forms, depend['name'])
            inns = get_inn(df, forms, depend['inn'])
            taxes_org = get_tax_org(df, forms, depend['taxes_org'])
            work_places = get_work_place(df, forms, depend['work_place'])
            work_titles = get_work_title(df, forms, depend['work_title'])
            incomes = get_incomes(df, individuals, depend['income'])
            spendings = get_spendings(df, individuals, depend['spendig'])
            rel_incomes = get_rel_incomes(df, close_relatives, depend['income'])
            rel_spendings = get_rel_spendings(df, close_relatives, depend['spendig'])

            real_estate(df, names, inns, taxes_org, work_places, work_titles, incomes, spendings, rel_incomes,
                        rel_spendings, depend, forms, individuals, close_relatives)

            persons, person_data, rel_data = real_estate(df, names, inns, taxes_org, work_places, work_titles, incomes,
                                                         spendings, rel_incomes, rel_spendings, depend, forms,
                                                         individuals, close_relatives)

            MY_CURSOR.execute('create table if not exists person( name_ text not null, '
                              'inn bigserial not null constraint person_pk primary key, '
                              'year_ int not null,'
                              'taxes_org text not null, '
                              'work_place text, '
                              'work_title text,'
                              'income real,'
                              'spending real,'
                              'rel_income real,'                              
                              'rel_spending real)')
            save_person(persons, depend['year'])

            MY_CURSOR.execute(f'create table if not exists person_data('
                              f'owner_id int, '
                              f'per_inn bigserial not null constraint person_data_person_fk references person, '
                              f'income float,'
                              f'income_type text,'
                              f'spending float,'
                              f'spending_type text,'
                              f'real_estate text,'
                              f'real_estate_sqr float,'
                              f'real_estate_sqr_type text,'
                              f'business_entity text,'
                              f'business_entity_share text,'
                              f'personal_estate_names text,'
                              f'personal_estate_info text,'
                              f'business_entity_personal_estate text,'
                              f'business_entity_share_personal_estate float,'
                              f'business_entity_share_personal_estate_type text)')
            MY_CURSOR.execute('create unique index if not exists no_dups_person on person_data( owner_id, '
                              'per_inn, '
                              'income, '
                              'income_type, '
                              'spending, '
                              'spending_type, '
                              'real_estate, '
                              'real_estate_sqr, '
                              'real_estate_sqr_type, '
                              'business_entity, '
                              'business_entity_share, '
                              'personal_estate_names, '
                              'personal_estate_info, '
                              'business_entity_personal_estate, '
                              'business_entity_share_personal_estate, '
                              'business_entity_share_personal_estate_type)')
            save_person_data(person_data)

            MY_CURSOR.execute(f'create table if not exists rel_data('
                              f'rel_owner_id int, '
                              f'rel_inn bigserial not null constraint rel_data_person_fk references person, '
                              f'rel_income float,'
                              f'rel_income_type text,'
                              f'rel_spending float,'
                              f'rel_spending_type text,'
                              f'rel_real_estate text,'
                              f'rel_real_estate_sqr float,'
                              f'rel_real_estate_sqr_type text,'
                              f'rel_business_entity text,'
                              f'rel_business_entity_share text,'
                              f'rel_personal_estate_names text,'
                              f'rel_personal_estate_info text,'
                              f'rel_business_entity_personal_estate text,'
                              f'rel_business_entity_share_personal_estate float,'
                              f'rel_business_entity_share_personal_estate_type text)')
            MY_CURSOR.execute('create unique index if not exists no_dups_relatives on rel_data( rel_owner_id, '
                              'rel_inn, '
                              'rel_income, '
                              'rel_income_type, '
                              'rel_spending, '
                              'rel_spending_type, '
                              'rel_real_estate, '
                              'rel_real_estate_sqr, '
                              'rel_real_estate_sqr_type, '
                              'rel_business_entity, '
                              'rel_business_entity_share, '
                              'rel_personal_estate_names, '
                              'rel_personal_estate_info, '
                              'rel_business_entity_personal_estate, '
                              'rel_business_entity_share_personal_estate, '
                              'rel_business_entity_share_personal_estate_type)')
            save_rel_data(rel_data)


main()
