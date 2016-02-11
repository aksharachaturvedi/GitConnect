from app import app
from flask import jsonify
from cassandra.cluster import Cluster
from flask import render_template
from flask import render_template, jsonify, request
from cassandra import ConsistencyLevel

#cluster = Cluster(['52.89.41.128', '52.89.60.184', '52.89.60.239', '52.89.26.208'])

cluster = Cluster(["ec2-52-89-26-208.us-west-2.compute.amazonaws.com",
                    "ec2-52-89-60-184.us-west-2.compute.amazonaws.com",
                    "ec2-52-89-60-239.us-west-2.compute.amazonaws.com",
                    "ec2-52-89-41-128.us-west-2.compute.amazonaws.com"])

keyspace = 'community'
followerrank = "followrank"
ccomponentlookup = "ccomponentlookup"
adjlist = "ccomponentadjlist"
userrank = "topusers"
userinfo = "userinfo"

session = cluster.connect(keyspace)
session.default_timeout = 120
# #xsession.default_consistency_level = ConsistencyLevel.QUORUM

followquery = "SELECT login, pagerank FROM %s " %(followerrank,)
usrrankquery = "SELECT user_id, rank FROM %s " %(userrank,)
cclookup = "SELECT component_id FROM %s WHERE vertex_id = ?" %(ccomponentlookup,)
adjlstquery = "SELECT user_id, listvertices FROM %s WHERE component_id = ?" %(adjlist,)
useridquery = "Select * from %s where user_id = ? " %(userinfo,)

giturl = "https://github.com/"

@app.route('/')
def home():
    return render_template("index.html")

@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/slides')
def slides():
    return render_template("slides.html")

@app.route('/followers')
def followerRank():
    sorted_results= getTopFollowered()
    followers = sorted_results[:5]
    return render_template("followers.html", followers=followers, giturl=giturl)

# @app.route('/api/graph/<login>')
# def get_data(login) :
#     adjlst=[]
#     node_names=[]
#     adjlst=getAdjList(login)
#     print "printing adj list"
#     for x in adjlst:
#         idname = getLoginFromId(long(x))
#         if idname:
#             node_names.append(idname)
#     print node_names
#     result = ', '.join(node_names)
#     jsonify(result)
#     results=[{'vertices':result}]


# @app.route('/graph/<login>')
@app.route('/graph',  methods=['POST'])
def graph():
    login=request.form["login"]
    adjlst=[]
    node_names=[]
    adjlst=getAdjList(login)
    print "printing adj list"
    for x in adjlst:
        idname = getLoginFromId(long(x))
        if idname:
            node_names.append(idname)
    print node_names
    result = ', '.join(node_names)
    results=[{'vertices':result}]

    return render_template("graph.html", adjlist=results, user=login)

@app.route('/users')
def userstop():
    users = getTopUsers()
    return render_template("users.html", users=users)

@app.route("/index", methods=['POST'])
def email_post():
    login = request.form["login"]
    node_names=[]
    adjlst=getAdjList(login)
    print "printing adj list"
    for x in adjlst:
        idname = getLoginFromId(long(x))
        if idname:
            node_names.append(idname)
    print node_names
    result = ', '.join(node_names)
    results = [{'vertices':result}]

    print results
    return render_template("graph.html")


def getTopUsers():
    res = session.execute(usrrankquery)
    response_list = []
    login = []

    for val in res:
        response_list.append(val)

    results = [{'user_id': x.user_id, "rank":x.rank} for x in response_list]
    sorted_results = sorted(results, key=lambda x:x["rank"], reverse=True)

    for i in sorted_results:
        astring = getLoginFromId(i['user_id'])
        print astring
        login.append(astring)

    users = login
    return users

def getLoginFromId(userId):
     useridquery = "SELECT user_name FROM userinfo WHERE user_id = %s"
     print "useridquery--> " + useridquery
     result = session.execute(useridquery, parameters=[userId])
     res = []

     for first_row in result:
        res = first_row.user_name

     return res

def getTopFollowered():
    res = session.execute(followquery)

    response_list = []
    for val in res:
        response_list.append(val)

    results = [{'login': x.login, "pagerank":x.pagerank } for x in response_list]
    sorted_result = sorted(results, key=lambda x:x["pagerank"], reverse=True)
    return sorted_result

def getIdFromLogin(login):
     print "in getIdFromLogin"

     useridquery = "SELECT user_id FROM userinfo WHERE user_name = %s"
     result = session.execute(useridquery, parameters=[login])
     res = []

     for first_row in result:
        res = first_row.user_id

     return res

def getAdjList(login):
    print "in getAdjList"
    uid = getIdFromLogin(login)

    if uid > 0 :
        ccId=getCcId(uid)

    if ccId > 0:
        ccquery = "SELECT verticeslst FROM community.ccomponentadjlist WHERE component_id = %s"
        result = session.execute(ccquery, parameters=[ccId])
        res = []
        for first_row in result:
            res = first_row.verticeslst
    else:
        print "error"
    return res

def getCcId(uid):
    ccquery =  "SELECT component_id FROM community.ccomponentlookup WHERE vertex_id = %s"
    result = session.execute(ccquery, parameters=[uid])
    res = []

    for first_row in result:
        res = first_row.component_id

    return res

def generateJsonFile(login, neighbors):
    import json
# {
#   "nodes":[
#     {"name":"Myriel","group":1},
#     {"name":"Napoleon","group":1}
# ],
#      "links":[
#     {"source":1,"target":0,"value":1},
#     {"source":2,"target":0,"value":8},
#     {"source":3,"target":0,"value":10},
#     {"source":3,"target":2,"value":6},
#     ]
# }

output = {'nodes':[], 'links':[]}
output['nodes'].append({
    'name': login,
    'group': 1
})



