import sqlite3
LABEL = 'LABEL'
PROPERTY = 'PROPERTY'
ID = 'ID___'
ID_IN = 'ID___IN'
ID_OUT = 'ID___OUT'
idpool = list(range(30))
_node_table = []
_relation_table = []
_id_table_map = {}
class GraphBase:
    ## \brief constructor
    def __init__(self):
        open('node.db','w').close()
        open('relation.db','w').close()
        self.connection = sqlite3.connect('node.db')
        self.cursor = self.connection.cursor()
        self.cursor.execute("ATTACH DATABASE ? AS relation",("relation.db"))
        self.connection.commit()
    ## \brief create a node in database
    # CREATE(p:P{name:'xxx'})  ==> createNode({'LABEL':'P',PROPERTY:{'name':'xxx'}})
    # \return id of created node
    def createNode(self,describer):
        if LABEL not in describer:
            type_ = "*"
        else:
            type_ = describer[LABEL]
        prop_ = describer[PROPERTY]
        try:
            prop_[ID] = idpool.pop(0)
            _id_table_map[prop_[ID]] = type_
        except:
            raise Exception("run out of resource")
        columns = []
        values = []
        for key in prop_:
            columns.append(str(key))
            values.append(str(prop_[key]))
        if type_ not in _node_table:
            _node_table.append(type_)
            query = "CREATE TABLE IF NOT EXISTS ? ("+",".join(["? TEXT" for i in range(len(columns))])+" )"
            param = tuple(["node."+type_]+columns)
        self.cursor.execute(query,param)

        for col in columns:
            query = "ALTER TABLE ? ADD ? TEXT"
            param = ("node."+type_,col)
            try:
                self.cursor.execute(query,param)
            except:
                pass
        query = "INSERT INTO ? ("+",".join(["?"for i in range(len(columns))])+")VALUES("+",".join(["?"for i in range(len(columns))])+")"
        param = tuple(["node."+type_]+columns+values)
        self.cursor.execute(query,param)
        self.connection.commit()
        return prop_['ID']
    ## \brief create node if not exists
    # \return id of the merged node
    def mergeNode(self,describer):
        res = self.where(describer):
        if res:
            return res[0]
        else:
            return self.createNode(describer)
    ## \brief delete node base on id
    # \return void
    def delete(self,ids):
        query = "DELETE FROM ? WHERE ? = ? OR ? = ?;"
        for id_ in ids:
            params = [("relation."+table,ID_IN,id_,ID_OUT,id_)for table in _relation_table]
            self.cursor.executemany(query,params)
            table_ = _id_table_map[id_]
            self.cursor.execute(query,("node."+table_,ID,id_,"FALSE","TRUE"))
        self.connection.commit()
    ## \brief search for node/relation
    # MATCH (a) WHERE a.x = 'c' RETURN a
    # where( { SEARCH_EVI : [(NODE,'a',*)], SEARCH_DESCRIBE : [( ('a','x'),'c','EQU')] , SEARCH_RET:[a] })
    # MATCH (a)-[b]->(c) WHERE b.d = 1 AND a.e > 2 return a,b,c
    # where({SEARCH_EVI : [(NODE,'a',*),(RELATION,'b',*),(NODE,'c',*)] ,SEARCH_DESCRIBE: [( ('b','d'),1,'EQU') ,(('a','e'),2,'GRE')],SEARCH_RET:[a,b,c] })
    def where(self,describer):
        return None

