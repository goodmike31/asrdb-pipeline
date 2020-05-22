from prefect import task, Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.tasks.shell import ShellTask
from os.path import join
import os
import sys
import inspect
import requests
import datetime
from pathlib import Path

myself = lambda: inspect.stack()[1][3]

test = ShellTask(name="print_date", command="date")

# TODO: read config with paths
# TODO: design tests for config file

dir_datalake = "/home/pnowak/development/data/asr/datalake"

# db_name_t = "clarin"
# db_lang_t = "pl-PL"
# url_t = "https://clarin-pl.eu/dspace/bitstream/handle/11321/237/Mobile.zip?sequence=2&isAllowed=y"
# db_name_t = "librispeech"
# db_lang_t = "en-US"
# url_t = "http://www.openslr.org/resources/12/dev-clean.tar.gz"
db_name_t = "crowdsourced_hq_arg_speech"
lang_t = "es-AR"
url_t = "http://www.openslr.org/resources/61/es_ar_male.zip"

# COMMON FUNCTIONS

# create a task via the task decorator
#@task(target="func_task_target.txt", checkpoint=True, result_handler=LocalResult(dir="~/.prefect"))
#def func_task():
#    return 99

#  #TODO add target file ".done" for given URL or target path"

def download_file(status_file, url, path_dl):
    """Download file for given URL into given directory."""

    if (not os.path.exists(path_dl) & os.path.exists(status_file)):
        print('Saving as:\n%s' % (path_dl))
        with open(path_dl, 'wb') as f:
            response = requests.get(url, stream=True)
            total = response.headers.get('content-length')

            if total is None:
                f.write(response.content)
            else:
                downloaded = 0
                total = int(total)
                for data in response.iter_content(chunk_size=max(int(total / 1000), 1024 * 1024)):
                    downloaded += len(data)
                    f.write(data)
                    done = int(50*downloaded/total)
                    sys.stdout.write('\r[{}{}]'.format('█' * done, '.' * (50-done)))
                    sys.stdout.flush()
        sys.stdout.write('\n')
        Path(status_file).touch()
        print('Succesfully saved as:\n%s' % (path_dl))
    else:
        print ("File already downloaded.")

# PREFECT TASKS
# @task


# with prefect.context(lang=lang, db_name=db_name):
#@task(target="test.done", checkpoint=True, result_handler=LocalResult(dir="~/.prefect"))
@task
def download(url, db_name, lang):
    """Download speech corpora archive.

    Download archive from given URL.
    Return path to downloaded archive.
    """
    # get name of the current prefect task
    task_name = myself()
    # check if URL download path is not empty
    assert(len(url) > 0)
    print('Requesting download:\n%s' % url)

    # create directory for download and target path for download function
    # consists of data lake directory, task name, name of DB, lang and date
    dt = datetime.datetime.now()
    date = dt.strftime("%Y%m%d")

    target_dir = join(dir_datalake, db_name, lang, task_name, date)

    if not os.path.exists(target_dir):
        print("Creating dir:\n%s" % (target_dir))
        os.makedirs(target_dir)

    # get filename, assuming it's located at the end of URL
    filename = os.path.basename(url)
    # truncate any extra URL parameters
    filename = filename.split("?")[0]
    # check if file name is valid
    # TODO check all forbidden charaters, length etc. - maybe some library?

    # create target file location from datalake dir and extracted filename
    path_dl = os.path.join(target_dir, filename)
    # TODO replace manually checked status file passed as argument with
    # native target with varying filename

    status_file = os.path.join(target_dir, filename, ".done")

    path_to_data = download_file(status_file, url, path_dl)

    return [path_to_data]
#@task
#def extract (path):

with Flow('ASRDB Pipeline') as flow:
    url = Parameter('url')
    db_name = Parameter('db_name')
    lang = Parameter('lang')


    path_data_raw = download(url, db_name, lang)
    #path_data_extracted = extract(path_data_raw)

# flow.visualize()

state = flow.run(parameters=dict(url=url_t, db_name=db_name_t, lang=lang_t))
