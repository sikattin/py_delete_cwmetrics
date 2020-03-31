import re
import boto3
import logging
import json
"""
1. {id: id, VolumeId: VolumeIdリテラルのアスキーコードのシーケンス , ascii: VolumeIdリテラルのアスキーコード合計, index: 当該metricsのindex番号}のリストを生成する
2. 1.をアスキーコード合計をキーにソートする
3. 2分探索で削除ボリュームが含まれた1. のindexをもとめる。
4. 3.で得られたindexの辞書には該当metricsのindex番号とidが記録されているため、まずはindex番号を用いてmetricsからpopする。
   次に対となるmetricsも削除する。ソートされていることを利用して、直前又は直後の辞書が対となるmetricsと想定できる。
   直前と直後のリストに対して VolumeIdの値が同一であることを条件にして対となるメトリクスを持つ辞書を特定する。
   特定できた後は、その辞書のindexにindex番号が入っているため、これを利用してメトリクスからpopする。
"""

is_end = 0
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def init_cwclient():
    """initialize boto3 cloudwatch client
    
    Returns:
        boto3.client: cloudwatch client object
    """
    client = boto3.client("cloudwatch")
    return client

def gen_ascii(string: str):
    """ascii code generator
    
    Args:
        string (str): literal
    Returns:
        int: ascii code of character
    """
    for char in string:
        yield ord(char)

def get_asciisum(string: str):
    """sum ascii code every char of literal
    
    Args:
        string (str): literal
    
    Returns:
        int: total ascii code of literal
    """
    nums = 0
    for i in gen_ascii(string):
        nums += i
    return nums

def get_asciilist(string: str):
    """create the sequence of literal ascii code
    
    Args:
        string (str): literal
    
    Returns:
        list: ordered ascii code sequence of literal
    """
    res = list()
    for i in gen_ascii(string):
        res += (i, )
    return res

def binary_search(seq: list, target):
    """binary search

    Args:
        seq (list): sorted list
        target : search target
    Return:
        int index number
    """
    low = 0
    high = len(seq) - 1
    while low <= high:
        mid = (low + high) // 2
        pick = seq[mid]['ascii']
        if pick == sum(target):
            ## 文字の構成が同じであれば、順序がバラバラでもTRUEになってしまう。
            ## abcd == dcba
            ## これに対応するために、VolumeIdでの比較も行うが、文字構成が同じ要素
            ## がいくつあるかがわからないため、これを解決して合致するものだけを返す必要が
            ## あることがひとつの課題
            if seq[mid]['VolumeId'] == target:
                return mid
            else:

        if pick > sum(target):
            high = mid -1
        else:
            low = mid + 1
    return None

def binary_search_multi(seq: list, target: str, count=None):
    """binary search until count times of targets are found

    Args:
        seq (list): sorted list
        target (str): search target
        count (int): search counts. default is None
    Returns:
        tuple index numbers
    """
    if count is None:
        count = 2
    indexes = tuple()
    found = 0
    low = 0
    high = len(seq) - 1
    while low <= high:
        mid = (low + high) // 2
        pick = seq[mid]['VolumeId']
        if pick == target:
            found += 1
            indexes += (mid,)
            if count <= found:
                return indexes
            continue
        if pick > target:
            high = mid -1
        else:
            low = mid + 1
    return None

def delete_metrics(metrics: list, key):
    """delete metrics based on key
    
    Args:
        metrics (list): metrics list of the specified widget
        key : delete key
    
    Returns:
        list or None: Succees to delete, return list, or None
    """
    idx = 0
    idx2 = None
    metrics_maps = [
        {
            "id": metric[-1]['id'],
            "VolumeId": [n for n in gen_ascii(metric[-1]['label'])],
            "ascii": get_asciisum(metric[-1]['label']),
            "index": i
        }
        for i, metric in enumerate(metrics)
    ]
    metrics_maps = sorted(metrics_maps, key=lambda x: x['ascii'])
    i = binary_search(metrics_maps, key)
    if i is not None:
        idx = metrics_maps[i]['index']
        idnum = int(re.match(r"\w(\d+)", metrics_maps[i]['id']).group(1))
        reobj = re.compile(r"\w{0}".format(idnum))
        if reobj.match(metrics_maps[i-1]['id']):
            idx2 = metrics_maps[i-1]['index']
        elif reobj.match(metrics_maps[i+1]['id']):
            idx2 = metrics_maps[i+1]['index']
        metrics.pop(idx)
        if idx2 is not None:
            metrics.pop(idx2)
        return metrics
    else:
        return None

def lambda_handler(event, context):
    for record in event['Records']:
        msgbody = record['body']
        msgbody = json.loads(msgbody)
        if msgbody['detail']['result'] == "deleted":
            volid = msgbody['resources'][0].split("/")[1]
            volid_ascii = get_asciilist(volid)
            dbname_prefix = record['messageAttributes']['DboardPrefix']['stringValue']
            dboards = tuple()
            widgets = list()
            metrics = list()
            newmetrics = list()
            dbody = {"widgets": []}

            client = init_cwclient()

            # get dashboard list
            res = client.list_dashboards(DashboardNamePrefix=dbname_prefix)['DashboardEntries']
            res = sorted(res, key=lambda x: x['DashboardName'])
            gen = (ele['DashboardName'] for ele in res)
            dboards = tuple(gen)
            
            # delete metrics method
            for dboard in dboards:
                if is_end:
                    break
                res = client.get_dashboard(DashboardName=dboard)
                res = json.loads(res['DashboardBody'])
                widgets = res['widgets']
                for widget in widgets:
                    metrics = widget['properties']['metrics']
                    newmetrics = delete_metrics(metrics, volid_ascii)
                    if newmetrics is not None:
                        logger.info("Dashboard: {0}, Widget: {1}, DeletedTarget: {2}" \
                            .format(dboard, widget['properties']['title'], volid)
                        )
                        is_end = 1
                        widget['properties']['metrics'] = newmetrics
                    dbody['widgets'].append(widget)
                client.put_dashboard()
