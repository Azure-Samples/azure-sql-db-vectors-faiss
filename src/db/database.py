import os
import pyodbc
import logging
import json
from .utils import Buffer, VectorSet, NpEncoder, DataSourceConfig

_logger = logging.getLogger("uvicorn")

class DatabaseEngineException(Exception):
    pass

class DatabaseEngine:
    def __init__(self) -> None:
        self._connection_string = os.environ["MSSQL"]
        self._index_id = None   

    def from_config(config:DataSourceConfig):
        db = DatabaseEngine()
        db._source_table_schema = config.source_table_schema
        db._source_table_name = config.source_table_name
        db._source_id_column_name = config.source_id_column_name
        db._source_vector_column_name = config.source_vector_column_name
        db._vector_dimensions = config.vector_dimensions             
        db.initialize_internal_variables()
        db.validate_database_objects()
        return db

    def from_id(id:int):
        db = DatabaseEngine()
        conn = pyodbc.connect(db._connection_string) 

        cursor = conn.cursor()  
        cursor.execute("""
            select 
                parsename(source_table_name, 2) as source_schema_name,
                parsename(source_table_name, 1) as source_table_name,
                id_column_name,
                vector_column_name,
                dimensions_count as vector_dimensions
            from 
                [$vector].[faiss] 
            where 
                id = ?
            and
                status = 'CREATED';""", id)
        row = cursor.fetchone()

        if (row == None):
            raise DatabaseEngineException(f"Index #{id} not found.")
        
        db._source_table_schema = str(row.source_schema_name)
        db._source_table_name = str(row.source_table_name)
        db._source_id_column_name = str(row.id_column_name)
        db._source_vector_column_name = str(row.vector_column_name)
        db._vector_dimensions = int(row.vector_dimensions)
        cursor.close()
        conn.close()
        
        db.initialize_internal_variables()
        db.validate_database_objects()

        return db
    
    def validate_config(self):
        c = {
            "table_schema": self._source_table_schema,
            "table_name": self._source_table_name,
            "id_column_name": self._source_id_column_name,
            "vector_column_name": self._source_vector_column_name,
            "vector_dimensions": self._vector_dimensions
        }

        _logger.info(f"Configuration: {json.dumps(c)}...")

        if (self._source_table_schema == None):
            raise DatabaseEngineException("Source table schema not defined.")
        
        if (self._source_table_name == None):
            raise DatabaseEngineException("Source table name not defined.")
    
        if (self._source_vector_column_name == None):
            raise DatabaseEngineException("Source vector column not defined.")
    
        if (self._source_id_column_name == None):
            raise DatabaseEngineException("Source id column not defined.")
    
        if (self._vector_dimensions == None):
            raise DatabaseEngineException("Expected number of dimensions for vector column not define.")    
        
    def initialize_internal_variables(self):    
        self.validate_config()
        self._source_table_fqname = f'[{self._source_table_schema}].[{self._source_table_name}]'
        self._target_table_name = f'{self._source_table_name}${self._source_vector_column_name}'
        # self._function1_fqname=f'[$vector].[find_similar${self._target_table_name}]'
        # self._function2_fqname=f'[$vector].[find_cluster${self._target_table_name}]'
        self._embeddings_table_fqname = f'[{self._source_table_schema}].[{self._target_table_name}]'
        # self._clusters_centroids_table_fqname = f'[$vector].[{self._target_table_name}$clusters_centroids]'
        # self._clusters_centroids_tmp_table_fqname = f'[$tmp].[{self._target_table_name}$clusters_centroids]'
        # self._clusters_table_fqname = f'[$vector].[{self._target_table_name}$clusters]'  
        # self._clusters_tmp_table_fqname = f'[$tmp].[{self._target_table_name}$clusters]'  
        
    def validate_database_objects(self):
        conn = pyodbc.connect(self._connection_string) 
        
        table_id = conn.execute("select object_id(?)", self._source_table_fqname).fetchval()
        if (table_id == None):
            raise DatabaseEngineException(f"Source table {self._source_table_fqname} not found.")
        
        column_id_id = conn.execute("select [column_id] from sys.columns where [object_id] = ? and [name] = ?", table_id, self._source_id_column_name).fetchval()
        if (column_id_id == None):
            raise DatabaseEngineException(f"Source table column {self._source_id_column_name} not found.")

        column_vector_id = conn.execute("select [column_id] from sys.columns where [object_id] = ? and [name] = ?", table_id, self._source_vector_column_name).fetchval()
        if (column_vector_id == None):
            raise DatabaseEngineException(f"Source table column {self._source_vector_column_name} not found.")
        
        conn.close()
        
    def initialize(self): 
        conn = pyodbc.connect(self._connection_string)
        try:        
            cursor = conn.cursor()  
            cursor.execute(f"""
                if schema_id('$vector') is null begin
                    exec('create schema [$vector] authorization dbo')
                end
                if schema_id('$tmp') is null begin
                    exec('create schema [$tmp] authorization dbo')
                end           
                if object_id('[$vector].[faiss]') is null begin
                    create table [$vector].[faiss]
                    (
                        [id] int identity not null,
                        [source_table_name] sysname not null,
                        [id_column_name] sysname not null,
                        [vector_column_name] sysname not null,
                        [item_count] int null,
                        [dimensions_count] int null,
                        [status] varchar(100) not null,
                        [updated_on] datetime2 not null,
                        primary key nonclustered ([id]),
                        unique nonclustered ([source_table_name], [vector_column_name])
                    )
                end             
            """)
            cursor.close()
            conn.commit()
        finally:
            conn.close()

    def create_index_metadata(self, force: bool) -> int:
        id = None
        conn = pyodbc.connect(self._connection_string) 

        try:
            cursor = conn.cursor()  
            
            id = cursor.execute("""
                select id from [$vector].[faiss] where [source_table_name] = ? and [vector_column_name] = ?;
                """,
                self._source_table_fqname,
                self._source_vector_column_name
            ).fetchval()
            if (id != None):
                if (force == False):
                    raise DatabaseEngineException(f"Index for {self._source_table_fqname}.{self._source_vector_column_name} already exists.")
                else:
                    _logger.info(f"Index creation forced over existing index {id}...")
            
            if (id == None):
                _logger.info(f"Registering new index...")
                id = cursor.execute("""
                    set nocount on;
                    insert into [$vector].[faiss] 
                        ([source_table_name], [id_column_name], [vector_column_name], [dimensions_count], [status], [updated_on])
                    values
                        (?, ?, ?, ?, 'INITIALIZING', sysdatetime());
                    select scope_identity() as id;
                    """,
                    self._source_table_fqname,
                    self._source_id_column_name,
                    self._source_vector_column_name,
                    self._vector_dimensions  
                ).fetchval()
            else:
                _logger.info(f"Updating existing index...")
                cursor.execute("""
                    update 
                        [$vector].[faiss] 
                    set
                        [status] = 'INITIALIZING',
                        [item_count] = null,                            
                        [updated_on] = sysdatetime()
                    where 
                        id = ?;
                    """,
                    id
                )

            cursor.commit()
        finally:
            conn.close()
        
        self._index_id = id
        return id
    
    def update_index_metadata(self, status:str):
        conn = pyodbc.connect(self._connection_string) 

        cursor = conn.cursor()  
        cursor.execute("""
            update 
                [$vector].[faiss] 
            set                
                [status] = ?                
            where 
                id = ?;""", 
            status, 
            self._index_id, 
            )
        conn.commit()

        cursor.close()
        conn.close()

    def finalize_index_metadata(self, vectors_count:int):
        conn = pyodbc.connect(self._connection_string) 

        cursor = conn.cursor()  
        cursor.execute("""
            update 
                [$vector].[faiss] 
            set
                [item_count] = ?,
                [dimensions_count] = ?,
                [status] = 'CREATED',                
                [updated_on] = sysdatetime()
            where 
                id = ?;""", 
            vectors_count, 
            self._vector_dimensions,
            self._index_id, 
            )
        conn.commit()

        cursor.close()
        conn.close()
    
    # def save_index(self, index_id:int, index_bin, vectors_count:int, dimension_count:int, data_version:int):
    #     CONFIG = self._configuration
    #     source_table_name = f'[{CONFIG["SCHEMA"]}].[{CONFIG["TABLE"]}]'
    #     id_column_name = CONFIG["COLUMN"]["ID"]
    #     vector_column_name = CONFIG["COLUMN"]["VECTOR"]
    #     conn = pyodbc.connect(self._connection_string) 

    #     cursor = conn.cursor()  
    #     cursor.execute("delete from [$vector].[faiss] where id = ?", index_id)
    #     conn.commit()

    #     cursor.execute("""
    #         insert into [$vector].[faiss] 
    #             ([id], [source_table_name], [id_column_name], [vector_column_name], [data], [item_count], [dimension_count], [data_version], [updated_on], [status])
    #         values 
    #             (?, ?, ?, ?, ?, ?, ?, ?, sysdatetime(), ?);""", 
    #         index_id, 
    #         source_table_name, 
    #         id_column_name, 
    #         vector_column_name, 
    #         index_bin, 
    #         vectors_count, 
    #         dimension_count, 
    #         data_version,
    #         "CREATED")
    #     conn.commit()

    #     cursor.close()
    #     conn.close()

    # def load_index(self, index_num: int):
    #     conn = pyodbc.connect(self._connection_string) 
    #     cursor = conn.cursor()  

    #     row = cursor.execute(f"select [data], [data_version] from [$vector].[faiss] where id = ?", index_num).fetchone()
    #     if row == None:
    #         return None, 0
    #     pkl = row[0]
    #     version = row[1]
    #     cursor.close()
    #     conn.close()

    #     return pkl, version
    
    def load_vectors_from_db(self):    
        conn = pyodbc.connect(self._connection_string) 
        cursor = conn.cursor()  
        current_version = cursor.execute("select change_tracking_current_version() as current_version;").fetchval()
        cursor.close()

        query = f"""
            select {self._source_id_column_name} as item_id, {self._source_vector_column_name} as vector from {self._source_table_fqname} 
        """
        buffer = Buffer()    
        result = VectorSet(self._vector_dimensions)
        conn = pyodbc.connect(self._connection_string) 
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
