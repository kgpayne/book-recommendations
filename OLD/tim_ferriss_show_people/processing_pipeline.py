import pandas as pd
from nameparser import HumanName

from dagster import (
    execute_pipeline, execute_solid, pipeline, solid
)


@solid
def read_show_notes(context):
    return pd.read_json(
        './show_notes_scraper/show_notes.jl',
        lines=True
    )


@solid
def explode_people_mentioned(context, df):
    return df.explode('people_mentioned')


@solid
def extract_mentioned_peoples_names(context, df):
    people_mentioned = df[df['people_mentioned'].notna()]
    people_mentioned = people_mentioned['people_mentioned'].apply(pd.Series)
    people_mentioned = people_mentioned.rename(
        columns=lambda x: 'mentioned_' + str(x)
    )
    # people_mentioned = people_mentioned.drop('mentioned_0', axis=1)
    return people_mentioned


def remove_fancy_quotes_and_dashes(df, column_name):
    translation_table = dict(
        [
            (ord(x), ord(y))
            for x, y in
            zip(u"‘’´“”–-",  u"'''\"\"--")]
    )
    df[column_name] = (
        df[column_name].str.translate(translation_table)
    )
    return df


def quotes_to_brackets(text):
    new_text = ''
    first_bracket = True
    for char in text:
        if char == '"' and first_bracket:
            new_text += '('
            first_bracket = False
        elif char == '"' and not first_bracket:
            new_text += ')'
            first_bracket = False
        else:
            new_text += char
    return new_text


def convert_quoted_nicknames_to_brackets(df, column_name):
    df[column_name] = (
        df[column_name].apply(lambda x: quotes_to_brackets(x))
    )
    return df


@solid
def remove_missing_names(context, df):
    # Shouldn't have to do this - problem with the scraper...
    df = df[df['mentioned_name'].notna()]
    return df


@solid
def clean_mentioned_peoples_full_names(context, df):
    df = remove_fancy_quotes_and_dashes(df, 'mentioned_name')
    df = convert_quoted_nicknames_to_brackets(df, 'mentioned_name')
    return df


@solid
def parse_mentioned_people_names(context, df):
    df['parsed_name'] = (
        df['mentioned_name'].apply(
            lambda x: HumanName(x).as_dict()
        )
    )
    parsed_names = df['parsed_name'].apply(pd.Series)
    parsed_names = parsed_names.rename(
        columns=lambda x: 'parsed_' + str(x)
    )
    return parsed_names


@pipeline
def process_show_notes():
    show_notes = read_show_notes()
    parsed_mentioned_people = (
        parse_mentioned_people_names(
            clean_mentioned_peoples_full_names(
                remove_missing_names(
                    extract_mentioned_peoples_names(
                        explode_people_mentioned(show_notes)
                    )
                )
            )
        )
    )
    return parsed_mentioned_people
