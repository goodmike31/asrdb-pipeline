from prefect import task, Flow, Parameter, utilities
from prefect.tasks.shell import ShellTask

test = ShellTask(name="print_date", command="date")

# Task for downloading file from provided URL. Return path to downloaded data
@task
def download(url_dl):
    assert(url_dl.len()>0)
    print("Downloading {}", url_dl)
    #path_to_data=""
    #return [path_dld]

with Flow('ETL') as flow:
    url_dl = Parameter('url_dl')
    download(url_dl)
#    mapped_result = map_fn.map(numbers)
#    reduced_result = reduce_fn(mapped_result)

#flow.visualize()
test_url_dl="https://clarin-pl.eu/dspace/bitstream/handle/11321/237/Mobile.zip?sequence=2&isAllowed=y"
state = flow.run(parameters=dict(url_dl=test_url_dl)
#print(state.result[h])

#assert state.result[reduced_result].result == 9
#print(state.result[e].result)
