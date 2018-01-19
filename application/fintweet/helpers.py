import json, random, string, re
from pprint import pprint
from flask import render_template, request
from application.config import Configuration


def object_list(template_name, query, paginate_by=20, **context):
    page = request.args.get('page')
    if page and page.isdigit():
        page = int(page)
    else:
        page = 1
    object_list = query.paginate(page, paginate_by)
    return render_template(template_name, object_list=object_list, **context)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Configuration.ALLOWED_EXTENSIONS


class DataTables():
    __source = None
    __paginationType = "full_numbers"
    __columns = []
    __html = ''
    __tableClass = ""
    __tableID = "DataTable-2014"
    __mData = []

    def __init__(self, tableClass="table", paginationType="full_numbers", source=None):
        self.__tableClass = tableClass
        self.__paginationType = paginationType
        self.__tableID = random.choice(string.ascii_uppercase)
        if not source:
            raise Exception("Invalid data source exception.")
        else:
            self.__source = source

    def setColumns(self, columns):
        self.__columns = columns

    def __renderColumns(self):
        html = '\n<colgroup>\n'
        cout = 0
        for i in self.__columns:
            html += '  <col class="con' + str(cout) + '" />\n'
            cout += 1
        html += '</colgroup>\n'
        html += '<thead>\n   <tr>\n'
        for i in self.__columns:
            html += '<th align="center" valign="middle" class="head' + list(i.keys())[0] + '">' + i[
                list(i.keys())[0]] + '</th>\n'
            self.__mData.append({"mData": str(list(i.keys())[0])})
        html += '  </tr>\n </thead>\n'
        return html

    def __renderTable(self):
        table = '<table class="dataTable ' + self.__tableClass + '" id="' + self.__tableID + '">'
        table += self.__renderColumns()
        table += ' <tbody></tbody>\n</table>\n'
        return table

    def __renderScript(self):
        javascript = '<script type="text/javascript">\njQuery(document).ready(function(){\n'
        javascript += '   oTable = jQuery("#' + self.__tableID + '").dataTable({\n'
        javascript += '      "sPaginationType": "' + self.__paginationType + '",\n'
        javascript += '      "bProcessing": false,\n'
        javascript += '      "sAjaxSource": "' + self.__source + '",\n'
        javascript += '      "bServerSide": true,\n'
        javascript += '      "aoColumns": ' + json.dumps(self.__mData)
        javascript += '\n   }); \n}); \n</script>'
        return javascript

    def render(self):
        html = self.__renderTable()
        html += self.__renderScript()
        return html


class Collections():
    # @var data list
    data = []
    # @var list columns - The actual columns of the data
    __columns = []
    # list of columns that will rendered to JSON
    __showColumns = []
    # Data Template
    template = {
        "aaData": [],
        "sEcho": 0,
        "iTotalRecords": 0,
        "iTotalDisplayRecords": 0
    }

    # Accept the data from the constructor
    def __init__(self, data):
        self.data = data
        self.__getColumns(self.data)

    # Get the data keys as default columns
    def __getColumns(self, data):
        self.__columns = data[0].keys()
        return None

    # Sets the user defined columns
    # If columns doesnt exists it will be ignored
    def setColumns(self, columns):
        self.__showColumns = columns

    def __formatData(self, data):
        columns = self.__validateColumns()
        for dt in data:
            dict = {}
            for cl in columns:
                dict[cl] = dt[cl]
            self.template["aaData"].append(dict)
        self.template["sEcho"] = 1
        self.template["iTotalRecords"] = len(data)
        self.template["iTotalDisplayRecords"] = len(data)
        return self.template

    def __validateColumns(self):
        if not self.__showColumns:
            return self.__columns
        else:
            valid = []
            for i in self.__showColumns:
                if i in self.__columns:
                    valid.append(i)
            return valid

    # Return all data in JSON Format
    def respond(self):
        data = self.__formatData(self.data)
        return json.dumps(data)


class ServerSideTable(object):
    '''
    Retrieves the values specified by Datatables in the request and processes
    the data that will be displayed in the table (filtering, sorting and
    selecting a subset of it).

    Attributes:
        request: Values specified by DataTables in the request.
        data: Data to be displayed in the table.
        column_list: Schema of the table that will be built. It contains
                     the name of each column (both in the data and in the
                     table), the default values (if available) and the
                     order in the HTML.
    '''

    def __init__(self, request, data, column_list):
        self.result_data = None
        self.cardinality_filtered = 0
        self.cardinality = 0

        self.request_values = request.values
        self.columns = sorted(column_list, key=lambda col: col['order'])

        rows = self._extract_rows_from_data(data)
        self._run(rows)

    def _run(self, data):
        '''
        Prepares the data, and values that will be generated as output.
        It does the actual filtering, sorting and paging of the data.

        Args:
            data: Data to be displayed by DataTables.
        '''
        self.cardinality = len(data)  # Total num. of rows

        filtered_data = self._custom_filter(data)
        self.cardinality_filtered = len(filtered_data)  # Num. displayed rows

        sorted_data = self._custom_sort(filtered_data)
        self.result_data = self._custom_paging(sorted_data)

    def _extract_rows_from_data(self, data):
        '''
        Extracts the value of each column from the original data using the
        schema of the table.

        Args:
            data: Data to be displayed by DataTables.

        Returns:
            List of dicts that represents the table's rows.
        '''
        rows = []
        for x in data:
            row = {}
            for column in self.columns:
                default = column['default']
                data_name = column['data_name']
                column_name = column['column_name']
                row[column_name] = x.get(data_name, default)
            rows.append(row)
        return rows

    def _custom_filter(self, data):
        '''
        Filters out those rows that do not contain the values specified by the
        user using a case-insensitive regular expression.

        It takes into account only those columns that are 'searchable'.

        Args:
            data: Data to be displayed by DataTables.

        Returns:
            Filtered data.
        '''

        def check_row(row):
            ''' Checks whether a row should be displayed or not. '''
            for i in range(len(self.columns)):
                if self.columns[i]['searchable']:
                    value = row[self.columns[i]['column_name']]
                    regex = '(?i)' + self.request_values['sSearch']
                    if re.compile(regex).search(str(value)):
                        return True
            return False

        if self.request_values.get('sSearch', ""):
            return [row for row in data if check_row(row)]
        else:
            return data

    def _custom_sort(self, data):
        '''
        Sorts the rows taking in to account the column (or columns) that the
        user has selected.

        Args:
            data: Filtered data.

        Returns:
            Sorted data by the columns specified by the user.
        '''

        def is_reverse(str_direction):
            ''' Maps the 'desc' and 'asc' words to True or False. '''
            if str_direction == 'desc':
                self.reverse = True
            self.reverse = False
            return True if str_direction == 'desc' else False

        if (self.request_values['iSortCol_0'] != "") and \
                (int(self.request_values['iSortingCols']) > 0):
            column_number = int(self.request_values['iSortCol_0'])
            column_name = self.columns[column_number]['column_name']
            sort_direction = self.request_values['sSortDir_0']
            # self.reverse = False
            # is_reverse(sort_direction)
            return sorted(data,
                          key=lambda x: (type(x[column_name]).__name__, x[column_name]),
                          reverse=is_reverse(sort_direction))
        else:
            return data

    def _custom_paging(self, data):
        '''
        Selects a subset of the filtered and sorted data based on if the table
        has pagination, the current page and the size of each page.

        Args:
            data: Filtered and sorted data.

        Returns:
            Subset of the filtered and sorted data that will be displayed by
            the DataTables if the pagination is enabled.
        '''

        def requires_pagination():
            ''' Check if the table is going to be paginated '''
            if self.request_values['iDisplayStart'] != "":
                if self.request_values['iDisplayLength'] != -1:
                    return True
            return False

        if not requires_pagination():
            return data

        start = int(self.request_values['iDisplayStart'])
        length = int(self.request_values['iDisplayLength'])

        if len(data) <= length:
            return data[start:]
        else:
            limit = -len(data) + start + length
            if limit < 0:
                return data[start:limit]
            else:
                return data[start]

    def output_result(self):
        '''
        Generates a dict with the content of the response. It contains the
        required values by DataTables (echo of the reponse and cardinality
        values) and the data that will be displayed.

        Return:
            Content of the response.
        '''
        output = {}
        output['sEcho'] = str(int(self.request_values['sEcho']))
        output['iTotalRecords'] = str(self.cardinality)
        output['iTotalDisplayRecords'] = str(self.cardinality_filtered)
        output['data'] = self.result_data
        return output
