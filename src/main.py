import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import loads as get_json
from threading import RLock
from bigrams_sql import Question
import psycopg2

lock = RLock()


def form_array_of_rows(bigrams):
    _format = "ROW('{0}', '{1}')"
    result = ""
    for i, bigram in enumerate(bigrams):
        if bigram.w2 == "" or bigram.w1 == "":
            continue
        result += _format.format(bigram.w1, bigram.w2)
        if not i == len(bigrams) - 1:
            result += ","
    return result


def form_insert_value(datum):
    dilemma_inserts = form_array_of_rows(datum.question_bigrams.dilemma)
    result_inserts = form_array_of_rows(datum.question_bigrams.result)
    total_inserts = form_array_of_rows(datum.question_bigrams.total)
    return """(
    """ + str(datum.qid) + """,
    '""" + datum.question_text + """',
    """ + str(datum.answers) + """,
    """ + str(datum.yeses) + """,
    ROW(ARRAY[{0}]::bigram[], ARRAY[{1}]::bigram[], ARRAY[{2}]::bigram[])
    )""".format(dilemma_inserts, result_inserts, total_inserts)


def fetch_request(server_id, q_id):
    try:
        question_data = requests.get("http://willyoupress_node{0}.will_you_press_etl_1:5000/{1}"\
                            .format(server_id, q_id))
    except requests.exceptions.Timeout:
        return "node{0} could not get data for {1}".format(server_id, q_id)
    except requests.exceptions.TooManyRedirects:
        return "node{0} could not get data for {1}".format(server_id, q_id)
    except requests.exceptions.RequestException:
        return "node{0} could not get data for {1}".format(server_id, q_id)
    question_data = get_json(question_data.text)
    return q_id, question_data


def main(conn_string):
    # setup db
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # some consts
    server = 1
    num_servers = 4 + 1 # for modulo purposes
    results = []
    executor = ThreadPoolExecutor(max_workers=4)

    # setup tasks
    for i in range(22, 22):  # 100):
        results.append(executor.submit(fetch_request, server, i))
        server = (server + 1) % num_servers
    for result in as_completed(results):
        qid, datum = result.result()
        # cursor.execute()

    qs = Question(1, 'This is a title', 5, 8, {'dilemma': [['first', 'second']], 'result': [[]],
                                              'total': [['second', 'forth'], ['eighty', 'another']]
                                              })
    sql = "INSERT INTO questions VALUES" + form_insert_value(qs)
    if cursor.execute(sql):
        print("success?")
    else:
        print("fail?")



# setup a data base
# has the master give the data base the results

if __name__ == "__main__":
    conn_string = "host='willyoupress_db_1' dbname='will_you_press' user='ian' password='ian'"
    main(conn_string)
