import sqlite3
LABEL = 'LABEL'
PROPERTY = 'PROPERTY'
idpool = list(range(30))
class GraphBase:
    def __init__(self):
        open('node.db','w').close()
        open('relation.db','w').close()
        self.nodedb_connection = sqlite3.connect('node.db')
        self.relationdb_connection = sqlite3.connect('relation.db')
        self.node_cursor = self.nodedb_connection.cursor()
        self.relation_cursor = self.relationdb_connection.cursor()
    def createNode(self,describer):
        if LABEL not in describer:
            type_ = "*"
        else:
            type_ = describer[LABEL]
        prop_ = describer[PROPERTY]
        prop_['ID'] = idpool.pop(0)
        columns = []
        values = []
        for key in prop_:
            columns.append(str(key))
            values.append(str(prop_[key]))
        query = "CREATE TABLE IF NOT EXISTS ? ("+",".join(["? TEXT" for i in range(len(columns))])+" )"
        param = tuple([type_]+columns)
        self.node_cursor.execute(query,param)

        for col in columns:
            query = "ALTER TABLE ? ADD ? TEXT"
            param = (type_,col)
            try:
                self.node_cursor.execute(query,param)
            except:
                pass
        query = "INSERT INTO ? ("+",".join(["?"for i in range(len(columns))])+")VALUES("+",".join(["?"for i in range(len(columns))])+")"
        param = tuple([type_]+columns+values)
        self.node_cursor.execute(query,param)
        self.nodedb_connection.commit()
        return prop_['ID']
    def mergeNode(self,describer):
        res = self.where(describer):
        if res:
            return res[0]
        else:
            return self.createNode(describer)
    def where(self,describer):
        return None

