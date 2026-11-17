from .parquet import query


def detail(lake,snapshot,day=None):
    sql='SELECT * FROM payload'
    parameters=()
    if day is not None:
        sql+=' WHERE day=?'
        parameters=(day,)
    return query(lake.files(snapshot),sql+' ORDER BY event_id',parameters)


def duration(lake,snapshot):
    return query(lake.files(snapshot),'SELECT day,COUNT(*) view_count,SUM(seconds) AS "seconds" FROM payload GROUP BY day ORDER BY day')


def export(lake,snapshot,destination):
    import json
    from pathlib import Path
    result=detail(lake,snapshot)
    target=Path(destination)
    with target.open('w',encoding='utf-8') as stream:
        for row in result:
            stream.write(json.dumps(row,ensure_ascii=False)+'\n')
    return {'rows':len(result),'snapshot':snapshot,'path':str(target)}
