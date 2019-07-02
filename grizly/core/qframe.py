from IPython.display import HTML, display

class QFrame:
    
    def __init__(self, schema='', table='', fields={}, getfields=[], expr=''):
        self.schema = schema
        self.table = table
        self.expr = expr
        self.fields = fields
        self.getfields = getfields
        self.inquotes = "'{}'"
        self.fieldattrs = ['type', 'group_by', 'where', 'expr']
        
    def validate_field_types(self, inputfields):
        inpfieldtypes = set([list(inputfields[k])[0] for k in inputfields.keys()])
        if inpfieldtypes <= set(self.fieldattrs):
            return True
        else:
            raise ValueError('stuff is not in content')
        
    def init_fields(self, fields):
        for field in fields:
            fields[field]['group_by'] = ''
            fields[field]['where'] = ''
        return fields
    
    def from_dict(self, dictionary):
        try:
            self.validate_field_types(dictionary)
            self.fields = dictionary
            fields = self.init_fields(self.fields)
            self.fields = fields
            return self
        except ValueError:
            print('Your columns do not have the right types')
    
    def groupby(self, fields):
        for field in fields:
            self.fields[field]['group_by'] = 'group'
        return self
    
    def agg(self, aggtype):
        if aggtype in ['sum', 'count']:
            for field in self.getfields:
                self.fields[field]['group_by'] = aggtype
            return self
        else:
            return print('Aggregation type must be sum or count')
        
    def to_html(self):
        from IPython.display import HTML, display
        html_table = '<table>'
        header = '\n'.join(['<th>{}</th>'.format(th) for th in self.fieldattrs])
        html_table += '<tr><th>{}</th></tr>'.format(header)
        for field in self.fields:
            html_table += '''<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
                '''.format(field
                           , self.fields[field]['type']
                          , self.fields[field]['group_by']
                          , self.fields[field]['where'])
        html_table+='</table>'
        display(HTML(html_table))
        
        
    def __getitem__(self, getfields):
        self.getfields.append(getfields)
        return QFrame(schema=self.schema, table=self.table, fields=self.fields, getfields=getfields)
        
    def __setitem__(self, key, value):
        d = {}
        d[key] = {}
        if isinstance(value, QFrame):
            value = value.expr
            d[key]['expr'] = value
            d[key]['type'] = 'num'
        if isinstance(value, dict):
            d[key] = value
        fields = self.init_fields(d)
        self.fields[key] = fields[key]
        return QFrame(schema=self.schema, table=self.table, fields=self.fields)
    
    def __add__(self, other):
        if isinstance(self.getfields, str):
            a = self.table +'.'+ self.getfields
        else:
            a = ''
        if isinstance(other, int):
            b = str(other)
        else:
            b = self.table +'.'+ other.getfields
            print(b)
        if self.expr == '':
            expr = a  + ' + ' + b
        else:
            expr = self.expr  + ' + ' + b
        return QFrame(expr=expr, table=self.table)
    
    def __sub__(self, other):
        if isinstance(self.getfields, str):
            a = self.table +'.'+ self.getfields
        else:
            a = ''
        if isinstance(other, int):
            b = str(other)
        else:
            b = self.table +'.'+ other.getfields
        if self.expr == '':
            expr = a  + ' - ' + b
        else:
            expr = self.expr  + ' - ' + b
        return QFrame(expr=expr, table=self.table)

    def __truediv__(self, other):
        expr = '(' + self.expr + ') / ' + other.column
        return QFrame(expr=expr)

    def __mul__(self, other):
        expr = '(' + self.expr + ') * ' + other.column
        return QFrame(expr=expr)
    
    def equality(self, other, sign):
        where = self.fields[self.getfields]['where']
        if isinstance(other, str):
            other = self.inquotes.format(other)
        condition = self.getfields + sign + str(other)
        if where == '':
            where = condition
        else:
            where += ' and ' + condition 
        self.fields[self.getfields]['where'] = where
    
    def __eq__(self, other):
        self.equality(other, '=')
        return self
    
    def __ne__(self, other):
        self.equality(other, '<>')
        return self

    def __and__(self, other):
        return self

def sql(qf):
    selects = ', '.join(list(qf.fields.keys()))
    
    wheres = []
    for field in qf.fields:
        if qf.fields[field]['where'] != '':
            wheres.append(qf.fields[field]['where'])
    groups = []
    values = []
    for field in qf.fields:
        if qf.fields[field]['group_by'] != '':
            if qf.fields[field]['group_by'] == 'group':
                groups.append(qf.table+'.'+field)
            if qf.fields[field]['group_by'] == 'sum':
                values.append('sum({}'.format(qf.table)+'.'+field+')')
    
    if values != []:
        values = ', '.join(values)
        sql = 'SELECT {}, {} FROM {}.{}'.format(selects, values, qf.schema, qf.table)
    else:
        sql = 'SELECT {} FROM {}.{}'.format(selects, qf.schema, qf.table)
    
    if wheres != []:
        wheres = ' and '.join(wheres)
        sql += ' WHERE {}'.format(wheres)
    
    if groups != []:
        groups = ', '.join(groups)
        sql += ' GROUP BY {}'.format(groups)
    return sql
