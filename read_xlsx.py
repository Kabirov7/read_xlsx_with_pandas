import pandas as pd
import config
import re
import psycopg2

only_dight = re.compile('[^0-9,]')
only_str = re.compile('[^а-яА-яa-zA-Z.% -]')

DB = psycopg2.connect(options=f'-c search_path={config.options}', database=config.database, user=config.user,
                      password=config.password, host=config.host, port=config.port)
MY_CURSOR = DB.cursor()



df = pd.read_excel('8E0A0100.xlsx', header=None, sheet_name=0)

nonchik = [
    '', '.', '31.12.2016.', '31.12.2016', '01.01.201614.03.2016', '0', ',,0', 'nan', 'Форд-Мондео, 2001г.в., 1999Б',
    '..', '-', 'NO INFO',
]

content = []
for i in range(1, df.index[-1]):
    name = str(df.iloc[i, 4]).strip()  # done
    inn = only_dight.sub('', str(df.iloc[i, 3]).strip())  # done
    year = only_dight.sub('', str(df.iloc[i, 2]).strip().replace(',', ''))  # done
    year = int(year)
    taxes_org = str(df.iloc[i, 10]).strip().replace('nan', '-')  # done
    work_place = str(df.iloc[i, 5]).strip()  # done
    work_title = str(df.iloc[i, 6]).strip()  # done
    #use per_income
    #use per_spending

    relatives_income = str(df.iloc[i, 38]).replace('31.12.2016.', '').replace('31.12.2016', '').replace('01.01.2016', '').replace('14.03.2016', '').replace('.', ',')  # done
    rel_income_type = str(df.iloc[i, 39])
    relatives_income = only_dight.sub('', relatives_income.replace('.', ','))  # done
    relatives_spending = str(df.iloc[i, 13]).replace('..', '.').replace('.', ',')  # done
    rel_spending_type = only_str.sub('', relatives_spending)  # done
    relatives_spending = only_dight.sub('', relatives_spending.replace('.', ','))  # done

    per_income = str(df.iloc[i, 36]).replace('.', ',')
    per_income_type = str(df.iloc[i, 37]).strip()
    per_income = only_dight.sub('', per_income.replace('.', ','))  # done
    per_spending = str(df.iloc[i, 11]).replace('.', ',')
    per_spending_type = '-'
    per_spending = only_dight.sub('', per_spending.replace('.', ','))  # done
    real_estate = str(df.iloc[i, 16]).strip().replace('\n', ' ')
    real_estate_sqr = str(df.iloc[i, 17]).replace('.', ',')  # done
    real_estate_sqr_type = only_str.sub('', real_estate_sqr)  # done
    real_estate_sqr = only_dight.sub('', real_estate_sqr.replace('.', ','))  # done
    business_entity = str(df.iloc[i, 20]).strip()  # done
    business_entity_share = str(df.iloc[i, 21]).strip()  # done
    personal_estate_names = str(df.iloc[i, 22]).strip()  # done
    personal_estate_info = str(df.iloc[i, 23]).strip()  # done
    business_entity_personal_estate = str(df.iloc[i, 24]).strip()  # done
    business_entity_share_personal_estate = str(df.iloc[i, 25]).replace('.', ',')
    business_entity_share_personal_estate_type = only_str.sub('', business_entity_share_personal_estate)
    business_entity_share_personal_estate = only_dight.sub('', business_entity_share_personal_estate.replace('.', ','))

    # rel_income = str(df.iloc[i, 4]).strip()  # if i will use relatives, it is done
    # rel_income_type = str(df.iloc[i, 4]).strip()  # if i will use relatives, it is done
    # rel_spending = str(df.iloc[i, 4]).strip()  # if i will use relatives, it is done
    # rel_spending_type = str(df.iloc[i, 4]).strip()  # if i will use relatives, it is done
    rel_real_estate = str(df.iloc[i, 26]).strip()  # done
    rel_real_estate_sqr = str(df.iloc[i, 27]).replace('.', ',')  # done
    rel_real_estate_sqr_type = only_str.sub('', rel_real_estate_sqr)  # done
    rel_real_estate_sqr = only_dight.sub('', rel_real_estate_sqr.replace('.', ','))  # done
    rel_business_entity = str(df.iloc[i, 30]).strip()  # done
    rel_business_entity_share = str(df.iloc[i, 31]).strip()  # done
    rel_personal_estate_names = str(df.iloc[i, 32]).strip()  # done
    rel_personal_estate_info = str(df.iloc[i, 33]).strip()
    rel_business_entity_personal_estate = str(df.iloc[i, 34]).strip()
    rel_business_entity_share_personal_estate = str(df.iloc[i, 35]).replace('.', ',')
    rel_business_entity_share_personal_estate_type = only_str.sub('', rel_business_entity_share_personal_estate)
    rel_business_entity_share_personal_estate = only_dight.sub('',
                                                               rel_business_entity_share_personal_estate.replace('.',
                                                                                                                 ','))

    if inn in nonchik:
        inn = 0

    else:
        inn = int(inn)

    if (relatives_income == 'nan') or (len(relatives_income) == 0):
        relatives_income = '0'

    elif relatives_income != 'nan':
        if len(relatives_income) != 1:
            while (relatives_income[-1] == '.') or (relatives_income[-1] == ','):
                relatives_income = relatives_income[:-1]
                if len(relatives_income) <= 1:
                    relatives_income = '0'
        elif (len(relatives_income) == 1 or len(relatives_income) == 0) or (relatives_income == '.') or (
                relatives_income == ','):
            relatives_income = '0'
    relatives_income = float(relatives_income.replace(',', '.'))

    if (relatives_spending == 'nan') or (len(relatives_spending) == 0):
        relatives_spending = '0'

    elif relatives_spending != 'nan':
        if len(relatives_spending) != 1:
            while (relatives_spending[-1] == '.') or (relatives_spending[-1] == ','):
                relatives_spending = relatives_spending[:-1]
                if len(relatives_spending) <= 1:
                    relatives_spending = '0'
        elif (len(relatives_spending) == 1 or len(relatives_spending) == 0) or (relatives_spending == '.') or (
                relatives_spending == ','):
            relatives_spending = '0'
    relatives_spending = float(relatives_spending.replace(',', '.'))

    if rel_spending_type in nonchik:
        rel_spending_type = '-'

    if (per_income == 'nan') or (len(per_income) == 0):
        per_income = '0'

    elif per_income != 'nan':
        if len(per_income) != 1:
            while (per_income[-1] == '.') or (per_income[-1] == ','):
                per_income = per_income[:-1]
                if len(per_income) <= 1:
                    per_income = '0'
        elif (len(per_income) == 1 or len(per_income) == 0) or (per_income == '.') or (
                per_income == ','):
            per_income = '0'
    per_income = float(per_income.replace(',', '.'))

    if per_income_type in nonchik:
        per_income_type = '-'

    if rel_income_type in nonchik:
        rel_income_type = '-'

    if (per_spending == 'nan') or (len(per_spending) == 0):
        per_spending = '0'

    elif per_spending != 'nan':
        if len(per_spending) != 1:
            while (per_spending[-1] == '.') or (per_spending[-1] == ','):
                per_spending = per_spending[:-1]
                if len(per_spending) <= 1:
                    per_spending = '0'
        elif (len(per_spending) == 1 or len(per_spending) == 0) or (per_spending == '.') or (
                per_spending == ','):
            per_spending = '0'
    per_spending = float(per_spending.replace(',', '.'))

    if real_estate in nonchik:
        real_estate = '-'

    if (real_estate_sqr == 'nan') or (len(real_estate_sqr) == 0):
        real_estate_sqr = '0'

    elif real_estate_sqr != 'nan':
        if len(real_estate_sqr) != 1:
            while (real_estate_sqr[-1] == '.') or (real_estate_sqr[-1] == ','):
                real_estate_sqr = real_estate_sqr[:-1]
                if len(real_estate_sqr) <= 1:
                    real_estate_sqr = '0'
        elif (len(real_estate_sqr) == 1 or len(real_estate_sqr) == 0) or (real_estate_sqr == '.') or (
                real_estate_sqr == ',') or (real_estate_sqr == '..0'):
            real_estate_sqr = '0'

        if real_estate_sqr in nonchik:
            real_estate_sqr = '0'
    real_estate_sqr = float(real_estate_sqr.replace(',', '.'))

    if real_estate_sqr_type in nonchik:
        real_estate_sqr_type = '-'

    if business_entity in nonchik:
        business_entity = '0'

    if business_entity_share in nonchik:
        business_entity_share = '0'

    if personal_estate_names in nonchik:
        personal_estate_names = '-'

    if personal_estate_info in nonchik:
        personal_estate_info = '0'

    if business_entity_personal_estate in nonchik:
        business_entity_personal_estate = '0'

    if (business_entity_share_personal_estate == 'nan') or (len(business_entity_share_personal_estate) == 0):
        business_entity_share_personal_estate = '0'

    elif business_entity_share_personal_estate != 'nan':
        if len(business_entity_share_personal_estate) != 1:
            while (business_entity_share_personal_estate[-1] == '.') or (
                    business_entity_share_personal_estate[-1] == ','):
                business_entity_share_personal_estate = business_entity_share_personal_estate[:-1]
                if len(business_entity_share_personal_estate) <= 1:
                    business_entity_share_personal_estate = '0'
        elif (len(business_entity_share_personal_estate) == 1 or len(business_entity_share_personal_estate) == 0) or (
                business_entity_share_personal_estate == '.') or (business_entity_share_personal_estate == ','):
            business_entity_share_personal_estate = '0'
    business_entity_share_personal_estate = float(business_entity_share_personal_estate.replace(',', '.'))

    if business_entity_share_personal_estate_type in nonchik:
        business_entity_share_personal_estate_type = '-'

    if rel_real_estate in nonchik:
        rel_real_estate = '-'

    if (rel_real_estate_sqr == 'nan') or (len(rel_real_estate_sqr) == 0):
        rel_real_estate_sqr = '0'

    elif rel_real_estate_sqr != 'nan':
        if len(rel_real_estate_sqr) != 1:
            while (rel_real_estate_sqr[-1] == '.') or (rel_real_estate_sqr[-1] == ','):
                rel_real_estate_sqr = rel_real_estate_sqr[:-1]
                if len(rel_real_estate_sqr) <= 1:
                    rel_real_estate_sqr = '0'
        elif (len(rel_real_estate_sqr) == 1 or len(rel_real_estate_sqr) == 0) or (rel_real_estate_sqr == '.') or (
                rel_real_estate_sqr == ',') or (rel_real_estate_sqr == '..0'):
            rel_real_estate_sqr = '0'

        if rel_real_estate_sqr in nonchik:
            rel_real_estate_sqr = '0'
    rel_real_estate_sqr = float(rel_real_estate_sqr.replace(',', '.'))

    if rel_real_estate_sqr_type in nonchik:
        rel_real_estate_sqr_type = '-'

    if rel_business_entity in nonchik:
        rel_business_entity = '0'

    if rel_business_entity_share in nonchik:
        rel_business_entity_share = '0'

    if rel_personal_estate_names in nonchik:
        rel_personal_estate_names = '-'

    if rel_personal_estate_info in nonchik:
        rel_personal_estate_info = '0'

    if rel_business_entity_personal_estate in nonchik:
        rel_business_entity_personal_estate = '0'

    if (rel_business_entity_share_personal_estate == 'nan') or (len(rel_business_entity_share_personal_estate) == 0):
        rel_business_entity_share_personal_estate = '0'

    elif rel_business_entity_share_personal_estate != 'nan':
        if len(rel_business_entity_share_personal_estate) != 1:
            while (rel_business_entity_share_personal_estate[-1] == '.') or (
                    rel_business_entity_share_personal_estate[-1] == ','):
                rel_business_entity_share_personal_estate = rel_business_entity_share_personal_estate[:-1]
                if len(rel_business_entity_share_personal_estate) <= 1:
                    rel_business_entity_share_personal_estate = '0'
        elif (len(rel_business_entity_share_personal_estate) == 1 or len(
                rel_business_entity_share_personal_estate) == 0) or (
                rel_business_entity_share_personal_estate == '.') or (rel_business_entity_share_personal_estate == ','):
            rel_business_entity_share_personal_estate = '0'
    rel_business_entity_share_personal_estate = float(rel_business_entity_share_personal_estate.replace(',', '.'))

    if rel_business_entity_share_personal_estate_type in nonchik:
        rel_business_entity_share_personal_estate_type = '-'


    row = [name, inn, year, taxes_org, work_place, work_title,
           per_income, per_spending, relatives_income, relatives_spending, per_income, per_income_type, per_spending, per_spending_type,
           real_estate, real_estate_sqr, real_estate_sqr_type, business_entity, business_entity_share, personal_estate_names, personal_estate_info,
           business_entity_personal_estate, business_entity_share_personal_estate, business_entity_share_personal_estate_type,
           #relative
           relatives_income, rel_income_type, relatives_spending, rel_spending_type, rel_real_estate, rel_real_estate_sqr, rel_real_estate_sqr_type,
           rel_business_entity, rel_business_entity_share, rel_personal_estate_names, rel_personal_estate_info, rel_business_entity_personal_estate,
           rel_business_entity_share_personal_estate, rel_business_entity_share_personal_estate_type
           ]

    if year == 2018 or year ==2017:
        content.append(row) 

MY_CURSOR.execute('create table if not exists declarations_old('
                  'name_ text not null, '
                  'inn bigserial not null, '
                  'year_ int not null, '
                  'taxes_org text not null, '
                  'work_place text, '
                  'work_title text, '
                  'income real, '
                  'spending real, '
                  'relations_income real, '
                  'relations_spending real, '
                  'per_income real,'
                  'per_income_type text,'
                  'per_spending real,'
                  'per_spending_type text,'
                  'real_estate text,'
                  'real_estate_sqr real,'
                  'real_estate_sqr_type text, '
                  'business_entity text, '
                  'business_entity_share text, '
                  'personal_estate_names text, '
                  'personal_estate_info text, '
                  'business_entity_personal_estate text, '
                  'business_entity_share_personal_estate real, '
                  'business_entity_share_personal_estate_type text, '
                  'rel_income real, '
                  'rel_income_type text, '
                  'rel_spending real, '
                  'rel_spending_type text, '
                  'rel_real_estate text, '
                  'rel_real_estate_sqr real, '
                  'rel_real_estate_sqr_type text, '
                  'rel_business_entity text, '
                  'rel_business_entity_share text, '
                  'rel_personal_estate_names text, '
                  'rel_personal_estate_info text, '
                  'rel_business_entity_personal_estate text, '
                  'rel_business_entity_share_personal_estate real, '
                  'rel_business_entity_share_personal_estate_type text '
                  ')')

MY_CURSOR.execute('create unique index if not exists no_dups_declarations_old on declarations_old( '
                  'name_, '
                  'inn, '
                  'year_, '
                  'income, '
                  'spending, '
                  'relations_income, '
                  'relations_spending, '
                  'work_title, '
                  'per_income, '
                  'per_spending, '
                  'real_estate, '
                  'real_estate_sqr, '
                  'business_entity, '
                  'business_entity_share, '
                  'personal_estate_names, '
                  'personal_estate_info, '
                  'business_entity_personal_estate, '
                  'business_entity_share_personal_estate, '
                  'business_entity_share_personal_estate_type, '
                  'rel_income, '
                  'rel_spending, '
                  'rel_real_estate, '
                  'rel_real_estate_sqr, '
                  'rel_business_entity, '
                  'rel_business_entity_share, '
                  'rel_personal_estate_names, '
                  'rel_personal_estate_info, '
                  'rel_business_entity_personal_estate, '
                  'rel_business_entity_share_personal_estate, '
                  'rel_business_entity_share_personal_estate_type'
                  ')')

for data in content:
    sql_formula = 'insert into declarations_old(' \
                      'name_, ' \
                      'inn, ' \
                      'year_, ' \
                      'taxes_org, ' \
                      'work_place, ' \
                      'work_title, ' \
                      'income, ' \
                      'spending, ' \
                      'relations_income, ' \
                      'relations_spending, ' \
                      'per_income, ' \
                      'per_income_type, ' \
                      'per_spending, ' \
                      'per_spending_type, ' \
                      'real_estate, ' \
                      'real_estate_sqr, ' \
                      'real_estate_sqr_type, ' \
                      'business_entity, ' \
                      'business_entity_share, ' \
                      'personal_estate_names, ' \
                      'personal_estate_info, ' \
                      'business_entity_personal_estate, ' \
                      'business_entity_share_personal_estate, ' \
                      'business_entity_share_personal_estate_type, ' \
                      'rel_income, ' \
                      'rel_income_type, ' \
                      'rel_spending, ' \
                      'rel_spending_type, ' \
                      'rel_real_estate, ' \
                      'rel_real_estate_sqr, ' \
                      'rel_real_estate_sqr_type, ' \
                      'rel_business_entity, ' \
                      'rel_business_entity_share, ' \
                      'rel_personal_estate_names, ' \
                      'rel_personal_estate_info, ' \
                      'rel_business_entity_personal_estate, ' \
                      'rel_business_entity_share_personal_estate, ' \
                      'rel_business_entity_share_personal_estate_type ' \
                      ') values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ' \
                      'on conflict do nothing'

    MY_CURSOR.execute(sql_formula, data)
    DB.commit()