from PySide6.QtCore import QObject, Slot, QUrl, Property, Signal, QAbstractTableModel, Qt, QAbstractListModel, QModelIndex, QStringListModel
from pathlib import Path

# Import dependent stuff
from sqlalchemy import engine
import pandas as pd

# Import whole classes
from ..quantlib.DataManifestManager import DataManifest
from ..quantlib.DataManager import DownloadIntraday, ExtractData
# Import functions in classes
from ..quantlib.SQLManager import SQLSetup, SQLEstablish, SQLRepair, SQLSave, SQLSync, SQLClear, SQLNuke, ExecuteSQL

#import utils # Import function to save outputdata
# TODO: CREATE A FUNCTION THAT TAKES OUTPUT FROM APP AND SAVES

### Backend is used by QML to interact with pure python code (mostly using wrappers)
# 
class Backend(QObject):
    def __init__(self):
        super().__init__()

        # Objects to hold
        self.data_manifest = None   # This holds DataManifest instance (and its path/engine as needed)
        self.engine = None          # This holds SQLAlchemy Engine (if given)

        self.base_path = Path(".").resolve() # Holds base path
        self.base_url = QUrl.fromLocalFile(str(self.base_path)) # Holds base path (in url form)

        self.curr_path_env = self.base_path # Initialise from base
        self.curr_path_json = self.base_path # Initialise from base

        # Models
        self.tabModel = TabListModel() # Model containing all newly-generated dataframes 
        self.dfModel = DataFrameModel(pd.DataFrame()) # DataManifest Model
        
        # Updaters setup (no '()' on functions)
        self.dataManifestChange.connect(self.updateAllParam)

        # Data-related objects
        self.allTickers_list = ["None"]
        self.allMonths_list = ["None"]
        self.allYears_list = ["None"]
        self.dataManifestChange.emit()

        

    ### Signals to track/update on events
    envPathChange = Signal()
    jsonPathChange = Signal()
    dataManifestChange = Signal()
    engineChange = Signal()
    modelsChanged = Signal()
    

    ### Methods for front-end state changes (i.e. value getters/setters) (text/plots etc.)
    # Paths/urls for filedialog (use path and convert to url as needed)
    # --- Env dialog (for SQL) --- #
    def _get_path_env(self):
        return self.curr_path_env
    
    def _set_path_env(self, new_path: str):
        pth = Path(new_path)
        if self.curr_path_env != pth:
            self.curr_path_env = pth
            self.envPathChange.emit()

    def _get_url_env(self):
        return QUrl.fromLocalFile(self.curr_path_env)
    
    def _set_url_env(self, new_path: QUrl):
        if QUrl.fromLocalFile(self.curr_path_env) != new_path:
            self.curr_path_env = new_path.toLocalFile()
            self.envPathChange.emit()
    
    # --- Json dialog (for DataManifest) --- #
    def _get_path_json(self):
        return self.curr_path_json
    
    def _set_path_json(self, new_path: str):
        pth = Path(new_path)
        if self.curr_path_json != pth:
            self.curr_path_json = pth
            self.jsonPathChange.emit()

    def _get_url_json(self):
        return QUrl.fromLocalFile(self.curr_path_json)
    
    def _set_url_json(self, new_path: QUrl):
        if QUrl.fromLocalFile(self.curr_path_json) != new_path:
            self.curr_path_json = new_path.toLocalFile()
            self.jsonPathChange.emit()

    def _get_strengine(self):
        return str(self.engine)

    # Checks if input exists in data manifest
    @Slot(str, int, str, result=bool)
    def _check_DataDM(self, ticker, interval, yearMonth) -> bool:
        """Returns boolean if data requested exists"""
        if self.data_manifest is not None:
            try:
                fileVal = self.data_manifest.DF.loc[((ticker,interval),yearMonth)]
                if fileVal == 1 or fileVal == 2:
                    return True
            except KeyError:
                pass
            except Exception as e:
                # log unexpected errors
                print(f"Unexpected error in _check_DataDM: {e}")

        return False
        
        

    ## NOTE: NEED TO MAKE SLOT ACCEPT LIST, ENGINE, DATAMANIFEST ETC.
    
    ### Slots for connecting variables in infrastructure to the back-end
    @Slot()
    def _newManifest(self):
        self.data_manifest = DataManifest()
        self.dfModel.setDataFrame(self.data_manifest.DF)
        self.updateAllParam()
        self.dataManifestChange.emit()
        return
    
    @Slot()
    def _delManifest(self): # Also deletes engine
        self.data_manifest = None
        self.engine = None
        self.dfModel.setDataFrame(pd.DataFrame())
        self.dataManifestChange.emit()
        return

    ### Slots for QML calling python methods and functions (using _ as methods are QML-Python connectors)
    #=== DataManifestManager Functions ===#
    @Slot(bool)
    def _validateManifest(self, fastValidate):
        if self.data_manifest is not None:
            return self.data_manifest.validateManifest(fastValidate = fastValidate, echo = True)

    @Slot(str, str)
    def _connectSQL(self, dbcred, path):
        if self.data_manifest is not None:
            self.data_manifest.connectSQL(dbcred = dbcred, path = path, echo = True)#
            self.engine = self.data_manifest.SQLengine
            self.engineChange.emit()
            return
        
    @Slot()
    def _reduceManifest(self):
        if self.data_manifest is not None:
            return self.data_manifest.reduceManifest()
        
    @Slot(str, int, str, int)
    def _setValue(self, ticker, interval, month, value):
        if self.data_manifest is not None:
            return self.data_manifest.setValue(ticker = ticker, interval = interval, month = month, value = value, sort = True)
        
    @Slot(str, int, str, bool, bool)
    def _loadData_fromcsv(self, ticker, interval, month, convert_DateTime, meta):
        if self.data_manifest is not None:
            # Direct output (just 1 DataFrame if meta=False, else tuple of 2 DataFrame)
            output = self.data_manifest.loadData_fromcsv(ticker = ticker, interval = interval, month = month, convert_DateTime = convert_DateTime, meta = meta, echo = True)
            if meta:
                main_df, meta_df = output
                self.tabModel.addTab(f"{ticker}/{month}/{interval} min (Stock Data)", main_df)
                self.tabModel.addTab(f"{ticker}/{month}/{interval} min (Meta Data)", meta_df)
            else:
                main_df = output
                self.tabModel.addTab(f"{ticker}/{month}/{interval} min (Stock Data)", main_df)

            self.modelsChanged.emit()
            return
        
    @Slot(str, int, str, bool, bool)
    def _loadData_fromsql(self, ticker, interval, month, convert_DateTime, meta) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
        if self.data_manifest is not None:
            if self.data_manifest.SQLengine is not None: # Just do not respond if no engine
                # Direct output (just 1 DataFrame if meta=False, else tuple of 2 DataFrame)
                output = self.data_manifest.loadData_fromsql(ticker = ticker, interval = interval, month = month, convert_DateTime = convert_DateTime, meta = meta, echo = True)
                if meta:
                    main_df, meta_df = output
                    self.tabModel.addTab(f"{ticker}/{month}/{interval} min (Stock Data)", main_df)
                    self.tabModel.addTab(f"{ticker}/{month}/{interval} min (Meta Data)", meta_df)
                else:
                    main_df = output
                    self.tabModel.addTab(f"{ticker}/{month}/{interval} min (Stock Data)", main_df)

                self.modelsChanged.emit()
                return
            else:
                print("No SQL engine. Not attempting load.")
    
    @Slot(str, str)
    def _saveManifest(self, saveTo, savePath):
        if self.data_manifest is not None:
            return self.data_manifest.saveManifest(saveTo = saveTo, savePath = savePath, echo = True)
        
    @Slot(str, str, str)
    def _loadManifest(self, loadFrom, path, fileName):
        if self.data_manifest is not None:
            self.data_manifest.loadManifest(loadFrom = loadFrom, path = path, fileName = fileName, echo = True)
            self.dfModel.setDataFrame(self.data_manifest.DF)
            self.updateAllParam()
            self.dataManifestChange.emit()
            return 

    #=== SQLManager Functions ===#
    # These functions use the SQLengine in self.data_manifest (where applicable)
    @Slot()
    def _SQLSetup(self):
        if self.data_manifest is not None and self.data_manifest.SQLengine is not None:
            return SQLSetup(self.data_manifest.SQLengine)
    
    @Slot()
    def _SQLEstablish(self):
        if self.data_manifest is not None and self.data_manifest.SQLengine is not None:
            return SQLEstablish(self.data_manifest.SQLengine)
    
    @Slot(bool, bool)
    def _SQLRepair(self, deepRepair, dataRepair):
        if self.data_manifest is not None and self.data_manifest.SQLengine is not None:
            return SQLRepair(dataManifest = self.data_manifest, deepRepair = deepRepair, dataRepair = dataRepair, echo = False)
    
    #@Slot(pd.DataFrame, engine, str, bool)
    #def _SQLSave(self, saveDF, connEngine, saveTable, ignore_index = True): # Ignore_index depends on input to save though...
    #    return SQLSave(saveDF, connEngine, saveTable, ignore_index = True, echo = False)

    @Slot(bool)
    def _SQLSync(self, fastSync):
        if self.data_manifest is not None and self.data_manifest.SQLengine is not None:
            return SQLSync(dataManifest = self.data_manifest, fastSync = fastSync, echo = False)

    @Slot()
    def _SQLClear(self):
        if self.data_manifest is not None and self.data_manifest.SQLengine is not None:
            return SQLClear(self.data_manifest.SQLengine, echo = False)

    @Slot()
    def _SQLNuke(self):
        if self.data_manifest is not None and self.data_manifest.SQLengine is not None:
            return SQLNuke(self.data_manifest.SQLengine, ignoreWarn = True) # Input() used for warning may raise error or issues so ignore (for now)

    @Slot(str, bool, bool)
    def _ExecuteSQL(self, query, fetch = False, transact = True):
        if self.data_manifest is not None and self.data_manifest.SQLengine is not None:
            return ExecuteSQL(query, self.data_manifest.SQLengine, fetch = fetch, transact = transact)

    #=== DataManager Functions ===#
    #@Slot(list,list,list, str, str, str, engine) #QVariant use [] to read as list?c
    #def _DownloadIntraday(self, tickers, intervals, months, APIkey, saveMode = 'direct'):
    #    if self.data_manifest != None:
    #        if saveMode == 'direct' or (saveMode in ['database', 'both'] and self.data_manifest.SQLengine != None):
    #            return DownloadIntraday(tickers, intervals, months, APIkey = APIkey, saveMode = saveMode, dataManifest = self.data_manifest, savePath = self.data_manifest.directory, connEngine = self.data_manifest.SQLengine, verbose = False)
    # TODO: ADD IN EXTRACT DATA AFTER CLEANING UP
    # SET CORRECT TYPES

    ##########################
    ### === Properties === ###
    ##########################
    @Property(bool, notify=dataManifestChange) # Allow QML to check if manifest exists
    def hasDataManifest(self):
        return self.data_manifest is not None
    
    #@Property(bool, notify=dataManifestChange) # Allow QML to check if SQLengine exists
    #def hasSQLEngine(self):
    #    return self.data_manifest is not None and self.data_manifest.SQLengine is not None
    
    # Getting months/tickers available in datamanifest (update first)
    @Property("QStringList", notify=dataManifestChange)
    def allTickers(self):
        if self.data_manifest is not None:
            if self.allTickers_list == []:
                return ["None"]
            return self.allTickers_list
        else:
            return ["None"]
    
    @Property("QStringList", notify=dataManifestChange)
    def allMonths(self):
        if self.data_manifest is not None:
            if self.allMonths_list == []:
                return ["--"]
            return self.allMonths_list
        else:
            return ["--"]
        
    @Property("QStringList", notify=dataManifestChange)
    def allYears(self):
        if self.data_manifest is not None:
            if self.allYears_list == []:
                return ["----"]
            return self.allYears_list
        else:
            return ["----"]
    
    ### Creating simpler properties for QML to read (for getting/setting variables)
    current_path_env = Property(str, fget = _get_path_env, fset = _set_path_env, notify = envPathChange)
    current_url_env = Property(QUrl, fget = _get_url_env, fset = _set_url_env, notify = envPathChange)
    current_path_json = Property(str, fget = _get_path_json, fset = _set_path_json, notify = jsonPathChange)
    current_url_json = Property(QUrl, fget = _get_url_json, fset = _set_url_json, notify = jsonPathChange)
    strEngine = Property(str, fget = _get_strengine, notify = engineChange)

    ########################
    ### === Updaters === ###
    ########################
    def updateAllParam(self):
        """Updates all tickers, months, years containing data from DataManifest"""
        if self.data_manifest is not None:
            self.allTickers_list = list(self.data_manifest.DF.index.get_level_values(0).unique().values)
            yearMonths = list(self.data_manifest.DF.columns.values)
            if yearMonths == []: # If empty, just make list empty to avoid error from zip
                self.allMonths_list = []
                self.allYears_list = []
            else:
                # Zip puts elements from different lists together, but first we split each date into ('year','month') in each element, so we get list[list[year,month]]
                # We make zip unpack the first list with *, then use zip to put all years into one list and all months into another list.
                years, months = zip(*(date.split("-") for date in yearMonths))
                self.allMonths_list = list(set(months))
                self.allYears_list = list(set(years))
                self.allMonths_list.sort()
                self.allYears_list.sort()



#############################################
### Modelling DataFrame (Table and Graph) ###
#############################################
# This modelling is done to expose data to app

# Override methods overrides the pure C++ methods

# Modelling DataFrames
class DataFrameModel(QAbstractTableModel):
    """A model to interface a Qt view with generic DataFrame. """

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.setDataFrame(df)
        self._roles = {Qt.DisplayRole: b"display"}

        # Flatten headers for MultiIndex into readable strings
        #if isinstance(df.columns, pd.MultiIndex):
        #    self._headers = [" | ".join(map(str, col)) for col in df.columns]
        #else:
        #    self._headers = [str(c) for c in df.columns]

    def roleNames(self):
        return self._roles

    def setDataFrame(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df
        self.headers = list(df.columns)
        self._rows, self._cols = df.shape
        self._vals = df.values
        print(f"Setting dataframe with shape: {self._rows} rows and {self._cols} columns.")
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()) -> int:
        """ Override method from QAbstractTableModel. Returns row count of the DataFrame. """
        return self._rows

    @Slot(result=int)
    def columnCount(self, parent=QModelIndex()) -> int:
        """ Override method from QAbstractTableModel. Returns column count of the DataFrame. """
        return self._cols

    def data(self, index, role) -> str:

        """ Override method from QAbstractTableModel. Returns data cell from the DataFrame. """
        if role == Qt.DisplayRole:
            return str(self._vals[index.row(), index.column()])
            #return str(self._df.iat[index.row(), index.column()])

    #@Property(str, constant=True)
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        """ Override method from QAbstractTableModel. Returns dataframe index as vertical header data and columns as horizontal header data. """
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return str(self._df.columns[section])
        else:
            import numpy as np
            val = self._df.index[section]
            
            # Normalize numpy scalars and tuples to string
            if isinstance(val, (np.generic)):
                return val.item()
            if isinstance(val, (tuple, list)):
                return " | ".join(map(str, val))
            return str(val)
            return str(self._df.index[section])

# Modelling list of dataframes
class TabListModel(QAbstractListModel):
    tabsChanged = Signal()

    NameRole = Qt.UserRole + 1
    ModelRole = Qt.UserRole + 2

    def __init__(self, tabs=None):
        super().__init__()
        self._tabs = tabs or []

    @Property(int, notify=tabsChanged)
    def rowCountQml(self, parent=QModelIndex()):
        return self.rowCount()
    
    def rowCount(self, parent=QModelIndex()):
        """Override method."""
        return len(self._tabs)

    def data(self, index, role):
        if not index.isValid():
            return None
        entry = self._tabs[index.row()]
        if role == TabListModel.NameRole:
            return entry.name
        if role == TabListModel.ModelRole:
            return entry.dfModel
        return None

    def roleNames(self):
        return {
            TabListModel.NameRole: b"name",
            TabListModel.ModelRole: b"dfModel",
        }
    
    #@Slot(str, QObject)
    def addTab(self, name, df):
        entry = TabEntry(name, DataFrameModel(df))
        self.beginInsertRows(QModelIndex(), len(self._tabs), len(self._tabs))
        self._tabs.append(entry)
        self.endInsertRows()


class TabEntry(QObject):
    def __init__(self, name, df_model, parent=None):
        super().__init__(parent)
        self._name = name
        self._df_model = df_model

    @Property(str, constant=True)
    def name(self):
        return self._name

    @Property(QObject, constant=True)
    def dfModel(self):
        return self._df_model
