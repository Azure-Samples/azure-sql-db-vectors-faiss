import os
import pyodbc
import logging
import json
from .utils import Buffer, VectorSet

_logger = logging.getLogger("uvicorn")

class DatabaseEngine:
    def __init__(self, configuration) -> None:
        self._connection_string = os.environ["MSSQL"]
        self._configuration = configuration       

    def initalize(self): 
        conn = pyodbc.connect(self._connection_string) 
        cursor = conn.cursor()  
        cursor.execute(f"""
            if schema_id('$vector') is null begin
                exec('create schema [$vector] authorization dbo')
            end
            if object_id('[$vector].[faiss]') is null begin
                create table [$vector].[faiss]
                (
                    [id] int not null,
                    [source_table_name] sysname not null,
                    [id_column_name] sysname not null,
                    [vector_column_name] sysname not null,
                    [data] varbinary(max) not null,
                    [item_count] int not null,
                    [dimension_count] int null,
                    [data_version] int not null default(0),
                    [status] varchar(100) not null,
                    [updated_on] datetime2 not null,
                    primary key ([id]),
                    unique nonclustered ([source_table_name], [vector_column_name])
                )
            end                                                              
        """)
        cursor.close()
        conn.commit()
        conn.close()

    def save_index(self, index_id:int, index_bin, vectors_count:int, dimension_count:int, data_version:int):
        CONFIG = self._configuration
        source_table_name = f'[{CONFIG["SCHEMA"]}].[{CONFIG["TABLE"]}]'
        id_column_name = CONFIG["COLUMN"]["ID"]
        vector_column_name = CONFIG["COLUMN"]["VECTOR"]
        conn = pyodbc.connect(self._connection_string) 

        cursor = conn.cursor()  
        cursor.execute("delete from [$vector].[faiss] where id = ?", index_id)
        conn.commit()

        cursor.execute("""
            insert into [$vector].[faiss] 
                ([id], [source_table_name], [id_column_name], [vector_column_name], [data], [item_count], [dimension_count], [data_version], [updated_on], [status])
            values 
                (?, ?, ?, ?, ?, ?, ?, ?, sysdatetime(), ?);""", 
            index_id, 
            source_table_name, 
            id_column_name, 
            vector_column_name, 
            index_bin, 
            vectors_count, 
            dimension_count, 
            data_version,
            "CREATED")
        conn.commit()

        cursor.close()
        conn.close()

    def load_index(self, index_num: int):
        conn = pyodbc.connect(self._connection_string) 
        cursor = conn.cursor()  

        row = cursor.execute(f"select [data], [data_version] from [$vector].[faiss] where id = ?", index_num).fetchone()
        if row == None:
            return None, 0
        pkl = row[0]
        version = row[1]
        cursor.close()
        conn.close()

        return pkl, version
    
    def load_vectors_from_db(self):    
        conn = pyodbc.connect(self._connection_string) 
        cursor = conn.cursor()  
        current_version = cursor.execute("select change_tracking_current_version() as current_version;").fetchval()
        cursor.close()

        query = self.__get_select_embeddings()
        buffer = Buffer()    
        result = VectorSet(self._configuration["VECTOR"]["DIMENSIONS"])
        cursor = conn.cursor()
        cursor.execute(query)
        tr = 0
        while(True):
            buffer.clear()    
            rows = cursor.fetchmany(10000)
            if (rows == []):
                _logger.info("Done")
                break

            for idx, row in enumerate(rows):
                buffer.add(row.item_id, json.loads(row.vector))
            
            result.add(buffer)
            tr += (idx+1)

            mf = int(result.get_memory_usage() / 1024 / 1024)
            _logger.info("Loaded {0} rows, total rows {1}, total memory footprint {2} MB".format(idx+1, tr, mf))        
            
        cursor.close()
        conn.commit()
        conn.close()
        return current_version, result.ids, result.vectors
    
    def get_changes(self, from_version:int = 0):
        EMBEDDINGS = self._configuration
        query = f"""
        declare @fromVersion int = ?
        declare @reason int = 0

        declare @curVer int = change_tracking_current_version();
        declare @minVer int = change_tracking_min_valid_version(object_id('[{EMBEDDINGS["SCHEMA"]}].[{EMBEDDINGS["TABLE"]}]'));

        -- Full rebuild needed
        if (@fromVersion < @minVer) begin
            set @reason = 2
        end

        -- No Changes
        if (@fromVersion = @curVer) begin
            set @reason = 1
        end

        if (@reason > 0)
        begin
            select
                @curVer as 'Metadata.Sync.Version',
                'None' as 'Metadata.Sync.Type',
                @reason as 'Metadata.Sync.ReasonCode'        
            for
                json path, without_array_wrapper
        end else begin
            declare @result nvarchar(max) = ((
            select
                @curVer as 'Metadata.Sync.Version',
                'Diff' as 'Metadata.Sync.Type',
                @reason as 'Metadata.Sync.ReasonCode',       
                [Data] = json_query((
                    select 
                        ct.SYS_CHANGE_OPERATION as '$operation',
                        ct.SYS_CHANGE_VERSION as '$version',
                        ct.[{EMBEDDINGS['COLUMN']['ID']}] as id, 
                        t.[{EMBEDDINGS['COLUMN']['VECTOR']}] as vector
                    from 
                        [{EMBEDDINGS["SCHEMA"]}].[{EMBEDDINGS["TABLE"]}] as t 
                    right outer join 
                        changetable(changes [{EMBEDDINGS["SCHEMA"]}].[{EMBEDDINGS["TABLE"]}] , @fromVersion) as ct on ct.[{EMBEDDINGS['COLUMN']['ID']}] = t.[{EMBEDDINGS['COLUMN']['ID']}]
                    for 
                        json path
                ))
            for
                json path, without_array_wrapper
            ))
            select @result as result
        end
        """
            
        conn = pyodbc.connect(self._connection_string)     
        cursor = conn.cursor()
        #print(from_version)
        cursor.execute(query, from_version)
        result = cursor.fetchone()
        result = json.loads(result[0])
        cursor.close()
        conn.close()
        return result

    def __get_select_embeddings(self):
        limit:int = int(os.environ["LIMIT_ROWS"] or -1) 
        config = self._configuration

        if (limit == -1):
            limit = None
        
        limit_query = ""
        if (limit):
            print(f"Limiting to {limit} rows...")
            limit_query = f"top({limit})"

        embeddings_table_name = f"[{config['SCHEMA']}].[{config['TABLE']}]"
        query = f"""
            select {limit_query} {config['COLUMN']['ID']} as item_id, {config['COLUMN']['VECTOR']} as vector from {embeddings_table_name} 
        """

        if (limit):
            query += " order by item_id"

        return query