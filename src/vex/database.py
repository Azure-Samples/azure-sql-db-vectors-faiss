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
            if object_id('[$vector].[index]') is null begin
                create table [$vector].[index]
                (
                    [id] int not null,
                    [type] varchar(50) not null,
                    [class] varchar(50) not null,
                    [data] varbinary(max) not null,
                    [item_count] int not null,
                    [vector_dimensions] int not null,
                    [data_version] int not null default 0,
                    [saved_on] datetime2 not null,
                    primary key ([id], [type])
                )
            end                                                              
        """)
        cursor.close()
        conn.commit()
        conn.close()

    def save_index(self, index_num:int, index_type, index_class, index_bin, vectors_count:int, dimensions_count:int, version:int):
        conn = pyodbc.connect(self._connection_string) 

        cursor = conn.cursor()  
        cursor.execute("delete from [$vector].[index] where id = ? and [type] = ?", index_num, index_type)
        conn.commit()

        cursor.execute("""
            insert into [$vector].[index] 
                ([id], [type], [class], [data], [item_count], [vector_dimensions], [data_version], [saved_on]) 
            values 
                (?, ?, ?, ?, ?, ?, ?, sysdatetime());""", 
            index_num, index_type, index_class, index_bin, vectors_count, dimensions_count, version)
        conn.commit()

        cursor.close()
        conn.close()

    def load_index(self, index_num: int, index_type):
        conn = pyodbc.connect(self._connection_string) 
        cursor = conn.cursor()  

        row = cursor.execute(f"select [data], [data_version] from [$vector].[index] where id = ? and [type] = ?", index_num, index_type).fetchone()
        if row == None:
            return None, 0
        pkl = row[0]
        version = row[1]
        cursor.close()
        conn.close()

        return pkl, version
    
    def load_vectors_from_db(self):    
        #_logger.info(f"Configuration: {self._configuration}")
        conn = pyodbc.connect(self._connection_string) 
        cursor = conn.cursor()  
        current_version = cursor.execute("select change_tracking_current_version() as current_version;").fetchval()
        cursor.close()

        query = self.__get_select_embeddings()
        cursor = conn.cursor()
        cursor.execute(query)
        buffer = Buffer()    
        result = VectorSet(self._configuration["VECTOR"]["DIMENSIONS"])
        while(True):
            buffer.clear()    
            rows = cursor.fetchmany(10000)
            if (rows == []):
                _logger.info("Done")
                break

            for idx, row in enumerate(rows):
                buffer.add(row.item_id, json.loads(row.vector))
            
            result.add(buffer)            
            mf = int(result.get_memory_usage() / 1024 / 1024)
            _logger.info("Loaded {0} rows, total memory footprint {1} MB".format(idx+1, mf))        
            
        # for idx, row in enumerate(rows):
        #     buffer.add(row.item_id, json.loads(row.vector))

        #     if (idx > 0 and (idx+1) % 10000 == 0):
        #         result.add(buffer)            
        #         mf = int(result.get_memory_usage() / 1024 / 1024)
        #         print("  Loaded {0} rows, memory footprint {1} MB".format(idx+1, mf))
        #         buffer.clear()     

        # if len(buffer.vectors) > 0:
        #     result.add(buffer)
        #     mf = int(result.get_memory_usage() / 1024 / 1024)
        #     print("  Loaded {0} rows, memory footprint {1} MB".format(idx+1, mf))

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
        limit:int = int(os.environ["LIMIT_ROWS"])
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