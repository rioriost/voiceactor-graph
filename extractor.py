#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
import re
import sys
import xmltodict

# third party library
try:
    from gremlin_python.driver import client, serializer, protocol
    from gremlin_python.driver.protocol import GremlinServerError
except ModuleNotFoundError:
    try:
        os.system('python3 -m pip install gremlinpython')
        from gremlin_python.driver import client, serializer, protocol
        from gremlin_python.driver.protocol import GremlinServerError
    except ModuleNotFoundError:
        print('Please install gremlinpython with brew: brew install gremlinpython')
        sys.exit(1)

# to extract the program titles from the text of a Wikipedia page
prog_title_re = re.compile(r"^\*+ \[\[(.+?)\]\]")

# Execute a Gremlin query and print results.
def execute_query(client, query):
    print("\n> {0}\n".format(query))
    try:
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tExecuted this query:\n\t{0}".format(callback.result().all().result()))
        else:
            print("Something went wrong with this query: {0}".format(query))
        print("\n")
        print("\tResponse status_attributes:\n\t{0}\n".format(callback.result().status_attributes))
    except GremlinServerError as e:
        if e.status_code == 409:
            print("\tAlready exists\n")
            pass

# Read the XML file and return a generator that yields the text of each page.
def file_read_generator(file_path: str, start_sep: str = '<page>', end_sep: str = '</page>') -> str:
    txt = ''
    in_page = False
    with open(file_path, 'r') as f:
        for line in f:
            if in_page == True:
                txt += line
                if end_sep in line:
                    yield txt
                    txt = ''
            else:
                if start_sep in line:
                    in_page = True
                    txt += line

# Extract the appearance list from the text of a Wikipedia page.
def extract_appearance_list(content: str) -> list:
    is_appear = False
    is_voice_actor = False
    txt = ''
    appearance_list = []
    for line in content.split('\n'):
        if '== 出演 ==' in line:
            is_appear = True
            continue
        if is_appear == True:
            if ('=== 声優 ===' in line) or ('=== テレビアニメ ===' in line) or ('=== 劇場アニメ ===' in line):
                is_voice_actor = True
                continue
        if is_voice_actor == True:
            if line == '':
                break
            m = prog_title_re.search(line)
            if m != None:
                g = m.groups()
                appearance_list.append(g[0].replace("'", "&quot;").replace("/", "&#047;").replace("?", "&#063;"))
    return list(set(appearance_list))

def main():
    # Create a Gremlin client.
    gr_client = client.Client('wss://riovagremlin.gremlin.cosmos.azure.com:443//', 'g',
                           username="/dbs/voice-actors/colls/actors-graph",
                           password="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx==",
                           message_serializer=serializer.GraphSONSerializersV2d0()
                           )
    # Drop the entire Graph
    execute_query(gr_client, "g.V().drop()")

    file_path = 'jawiki-20231201-pages-articles-multistream.xml'
    for page in file_read_generator(file_path):
        if ('Category:日本の男性声優' in page) or ('Category:日本の女性声優' in page):
            page_dict = xmltodict.parse(page)
            actor = page_dict['page']['title'].replace("'", "&quot;")
            # extract appearance list
            appearance_list = extract_appearance_list(page_dict['page']['revision']['text']['#text'])
            # Firstly add vertex of voice actor
            query = "g.addV('actor').property('id', '{0}').property('label', '{1}').property('pk', 'pk')".format(actor, actor)
            execute_query(gr_client, query)
            # Next, add vertices of appearance and edges
            for appearance in appearance_list:
                id = hashlib.md5(appearance.encode()).hexdigest()
                query = "g.addV('appearance').property('id', '{0}').property('label', '{1}').property('pk', 'pk')".format(id, appearance)
                execute_query(gr_client, query)
                query = "g.V('{0}').addE('has').to(g.V('{1}'))".format(actor, id)
                execute_query(gr_client, query)

if __name__ == "__main__":
    main()
