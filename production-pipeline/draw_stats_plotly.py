"""A quick code to make plotly stats from DB"""

import os
import time
import plotly.plotly as py
import plotly.graph_objs as go
from MongoDbUtil import MongoDbUtil

def main():
    """A quick code to make plotly stats from DB"""

    database = MongoDbUtil('ro').database()

    tag = 'Px1id'
    daemons = ['daq_files_watcher', 'jobs_validator', 'submitter']
    colls = ['%s_%s'%(coll, tag) for coll in daemons]

    datas = []
    for daemon, coll in zip(daemons, colls):
        last_doc = database[coll].find().skip(database[coll].count()-1)[0]
        accum_stats = last_doc['accum_stats']

        vals = {}
        timestamps = []
        for key in accum_stats.keys():
            vals[key] = []

        for doc in database[coll].find():
            timestamps.append(doc['date'])
            for key in vals:
                vals[key].append(doc['accum_stats'][key])

        urls = []
        for key in vals:
            urls.append(draw(timestamps, vals[key], daemon, key))

        datas.append({'title': daemon, 'urls': urls})

    make_index_file(tag, datas)

def make_index_file(tag, datas):
    now = time.strftime("%c")
    time_zone = time.strftime("%Z")

    os.system('rm -f index.md index.html')

    with open('index.md', 'w') as f_index:
        f_index.write('Pipeline for %s production  \n\n'%tag)
        f_index.write('Last updated on %s %s  \n\n'%(now, time_zone))

        for data in datas:
            f_index.write('---  \n')
            f_index.write('###%s  \n'%data['title'].replace('_', ' '))
            for url in data['urls']:
                f_index.write('<iframe width="900" height="800" frameborder="0" scrolling="no" src="%s"></iframe>  \n'%url)

    os.system('markdown index.md > index.html')
    os.system('chmod a+rw index.md')
    os.system('chmod a+rw index.html')


def draw(timestamps, val, daemon, ytitle):
    """Draw plotly and return url to plot"""

    data = [go.Scatter(x=timestamps, y=val, line=dict(color=('rgb(205, 12, 24)'), width=4))]

    layout = dict(title='%s - %s'%(daemon.replace('_', ' '), ytitle.replace('_', ' ')),
                  xaxis=dict(rangeselector=get_rangeselector(), rangeslider=dict(), type='date'),
                  yaxis=dict(title=ytitle.replace('_', ' '), titlefont=dict(family='Courier New, monospace', size=18, color='#000000')))

    fig = go.Figure(data=data, layout=layout)
    return py.plot(fig, filename='%s_%s'%(daemon, ytitle), auto_open=False)

def get_rangeselector():

    return dict(buttons=list([
                            dict(count=1,
                                 label='1h',
                                 step='hour',
                                 stepmode='backward'),
                            dict(count=6,
                                 label='6h',
                                 step='hour',
                                 stepmode='backward'),
                            dict(count=12,
                                label='12hr',
                                step='hour',
                                stepmode='todate'),
                            dict(count=1,
                                label='1d',
                                step='day',
                                stepmode='backward'),
                            dict(step='all')
                            ]))

if __name__ == '__main__':
    main()
