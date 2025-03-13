import urllib
import json
from argparse import ArgumentParser
from pathlib import Path
import urllib.request
import zipfile
import pandas as pd
import sys
import inspect
import math
import nltk
import random 

arg_parser = ArgumentParser(
    prog='fact',
    description='download and process fact data for the patient service',
)
arg_parser.add_argument('-c', '--config_file', type=str, default='./fixture_config.json', help='path to a json config file')
arg_parser.add_argument('-d', '--download', type=bool, default=True, help='download the assets given in config.json')
arg_parser.add_argument('-t', '--transform', type=bool, default=True, help='transform the data assets into the required fact tables')


def resolved_path(path_str, for_output=False):
    # ensure we can resolve any relative path in config so is relative to this script
    resolved_path = (Path(__file__).parent / Path(path_str)).resolve()
    # output paths should have all intermediate parent dirs created
    if for_output:
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return resolved_path


def download(config):
    print("Begin download task...")
    # iterate over all assets mention in the config download block
    for download_block in config['download']:
        # extract the source url
        download_url = download_block['url']
        # resolve the download destination path relative to this module, in case relative paths are used
        download_filepath = resolved_path(download_block['filepath'], for_output=True)
        print(f"Download {download_filepath.name}")
        # check to see if we already have this file
        if not download_filepath.exists():
            try:
                # fetch the data by opening the url
                print(f" - Fetching {download_url}")
                with urllib.request.urlopen(download_url) as response:
                    print(f" - Writing to {download_filepath}")
                    with open(download_filepath, 'wb') as f:
                        f.write(response.read())
            # Errors from the fetch
            except urllib.error.HTTPError as e:
                print(e.code)
                print(e.read())
                raise Exception(f" - Failed to fetch {download_url}")
            # Other Errors, including those around IO
            except OSError as e:
                print(e)
                raise Exception(f" - Failed to write {download_filepath}")
        else:
            print(f" - Already cached {download_filepath}")
        if download_filepath.suffix == '.zip':
            print(f" - Unzip {download_filepath}")
            with zipfile.ZipFile(download_filepath, 'r') as f:
                f.extractall(download_filepath.parent)


def snomed_transform(input=None, output=None):
    # read the input excel sheet and keep only relevant columns
    df = pd.read_excel(input)[['Description', 'Usage']]
    # only snowmed codes whose description mentions a disorder should be kept
    df = df[df.Description.str.endswith('(disorder)')]
    # trim this from the string
    df['disease'] = df.Description.str.replace(' (disorder)', '')
    # replace * values with a 1; assumption: these are uncommon codes
    with pd.option_context('future.no_silent_downcasting', True):
        usage_series = df.Usage.replace('*', 1)
        df['usage'] = pd.to_numeric(usage_series)
    # ensure relative paths are resolved
    df_path = resolved_path(output, for_output=True)
    # save the table
    df[['disease', 'usage']].to_parquet(df_path, index=False)
    print(f' - Written {df_path}')


def postcode_transform(nspl_fact_table_filename, ttwa_dimension_filename, rgn20cd_dimension_filename, city_table_output, region_table_output):
    # load the nspl table and take the relevant columns
    df = pd.read_csv(resolved_path(nspl_fact_table_filename), usecols=['ctry', 'pcds', 'doterm', 'rgn', 'ttwa'])
    # load the city lookup and set index
    city_dim = pd.read_csv(resolved_path(ttwa_dimension_filename))[['TTWA11CD', 'TTWA11NM']].set_index('TTWA11CD')
    # load the region lookup and set the index
    rgn_dim = pd.read_csv(resolved_path(rgn20cd_dimension_filename))[['RGN20CD', 'RGN20NM']].set_index('RGN20CD')
    # for some reason 'Wales' is actually '(pseudo) Wales', so fix that
    rgn_dim['RGN20NM'] = rgn_dim['RGN20NM'].str.replace('(pseudo) ', '')
    # we want to consider only england and wales
    df = df[df.ctry.isin(['E92000001', 'W92000004'])]
    # join the region name table with the postcode df, add better names
    df = df.join(rgn_dim, on='rgn').rename(columns={'RGN20NM':'region', 'pcds':'postcode'})
    # join the city column with the city lookup and provide friendly names
    df = df.join(city_dim, on='ttwa').rename(columns={'TTWA11NM': 'city'})
    # need a map from city to region, since postcodes move this might not be perfect so take the most common value
    rgn_df = df[['city', 'region']].groupby('city')['region'].agg(pd.Series.mode).reset_index()
    # ensure relative paths are fixed
    rgn_path = resolved_path(region_table_output, for_output=True)
    # save the region output
    rgn_df[['city', 'region']].to_parquet(rgn_path, index=False)
    print(f' - Written {rgn_path}')
    # the city df will be used for data generation, so use only non-terminated postcodes
    city_df = df[df.doterm.isna()]
    # make sure we have the correct path
    city_path = resolved_path(city_table_output, for_output=True)
    # write the city table
    city_df[['postcode', 'city']].to_parquet(city_path, index=False)
    print(f' - Written {city_path}')


def forenames_transform(input, number, output):
    df = pd.read_csv(resolved_path(input))
    df_list = []
    # split the list evenly by gender so we will also end up with an even split
    for n_gender, df_gender in [
        (math.ceil(number / 2), df[df.sex == 'boy']),
        (math.floor(number / 2), df[df.sex == 'girl'])
    ]:
        # take the year of maximum prevalence and then sort ascending to provide a list of unusual names
        df_list.append(df_gender.groupby('name').percent.max().sort_values(ascending=True).head(n_gender))
    # join the girls and boys list, reset the index because this is where the names have gone and remove prevalence
    forenames = pd.concat(df_list).reset_index()[['name']].rename(columns={'name':'forename'})
    # ensure relative paths are resolved
    names_path = resolved_path(output, for_output=True)
    # save
    forenames.to_parquet(names_path)
    print(f' - Written {names_path}')    


def surnames_transform(number, output):
    nltk.download('wordnet')
    nouns = [ss.lemma_names()[0] for ss in nltk.corpus.wordnet.all_synsets('n')]
    nouns = [s for s in nouns if '-' not in s and '_' not in s and s.lower()==s]
    surnames = [ f'{x.title()}-{y.title()}' for x,y in zip(random.sample(nouns, k=number), random.sample(nouns, k=number))]
    out_path = resolved_path(output, for_output=True)
    pd.DataFrame({'surname': surnames}).head(number).to_parquet(out_path)
    print(f' - Written {out_path}')


def sample_data_transform(number, city_filename, snomed_filename, forenames_filename, surnames_filename, output):
    forenames = pd.read_parquet(forenames_filename).sample(n=number).reset_index(drop=True)
    surnames = pd.read_parquet(surnames_filename).sample(n=number).reset_index(drop=True)
    df_ls = []
    df_ls.append(pd.DataFrame({'patient_name': forenames.forename + ' ' + surnames.surname}))
    df_ls.append(pd.read_parquet(city_filename).sample(n=number).reset_index(drop=True))
    df_ls.append(pd.read_parquet(snomed_filename).sample(n=number, weights='usage').reset_index(drop=True)[['disease']])
    sample_df = pd.concat(df_ls, axis='columns').reset_index(drop=False).rename(columns={'index': 'patient_id'})
    out_path = resolved_path(output, for_output=True)
    sample_df[['patient_id', 'patient_name', 'city', 'postcode', 'disease']].to_json(out_path, orient='records', lines=True)
    print(f' - Written {out_path}')
    

if __name__ == '__main__':
    args = arg_parser.parse_args()
    with open(args.config_file, 'r') as f:
        config = json.load(f)
    if args.download:
        download(config)
    if args.transform:
        # inspect this module and map names to members
        this_module = dict(inspect.getmembers(sys.modules['__main__']))
        for xf in config['transform']:
            name, params = xf['function'], xf['params']
            # check that our config contains things we can call, in particular ensure names end in "_transform"
            if not name.endswith('_transform'):
                raise Exception(f'call to {name} is not allowed, transforms must end in "_transform"')
            elif name not in this_module:
                raise Exception(f'call to {name} not found')
            elif not inspect.isfunction(this_module[name]):
                raise Exception(f'{name} is not a function')
            else: 
                print(f'Begin {name}...')
                this_module[name].__call__(**params)
    print('Finished')