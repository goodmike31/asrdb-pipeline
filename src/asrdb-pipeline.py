from prefect import task, Flow, Parameter
from prefect.tasks.shell import ShellTask
from os.path import join
import os
import sys
import inspect
import requests
import datetime

myself = lambda: inspect.stack()[1][3]

test = ShellTask(name="print_date", command="date")

# TODO: read config with paths
# TODO: design tests for config file

dir_datalake = "/home/pnowak/development/data/asr/datalake"


def download_file(url, target_dir):
    """Download file for given URL into given directory."""

    print("Creating dir:\n%s" % (target_dir))
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    filename = os.path.basename(url)
    path_dl = os.path.join(target_dir, filename)
    status_file = filename + ".done"

    print('Saving as:\n%s' % (path_dl))
    if (not os.path.exists(path_dl) & os.path.exists(status_file)):
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

@task
def download(url_dl, db_name, lang):
    """Download speech corpora archive.

    Download archive from given URL.
    Return path to downloaded archive.
    """
    # get name of the current prefect task
    task_name = myself()
    # check if URL download path is not empty
    assert(len(url_dl) > 0)
    print('Downloading:\n%s' % url_dl)

    # create directory for download and target path for download function
    # consists of data lake directory, task name, name of DB, lang and date
    dt = datetime.datetime.now()
    date = dt.strftime("%Y%m%d")

    path_dl = join(dir_datalake, task_name, db_name, lang, date)
    path_to_data = download_file(url_dl, path_dl)

    return [path_to_data]

with Flow('ASRDB Pipeline') as flow:
    url_dl = Parameter('url_dl')
    db_name = Parameter('db_name')
    lang = Parameter('lang')
    download(url_dl, db_name, lang)

# flow.visualize()
# test_db_name = "clarin"
# test_db_lang = "pl-PL"
# test_url_dl = "https://clarin-pl.eu/dspace/bitstream/handle/11321/237/Mobile.zip?sequence=2&isAllowed=y"
test_db_name = "librispeech"
test_db_lang = "en-US"
test_url_dl = "http://www.openslr.org/resources/12/dev-clean.tar.gz"

state = flow.run(parameters=dict(url_dl=test_url_dl, db_name=test_db_name, lang=test_db_lang))
