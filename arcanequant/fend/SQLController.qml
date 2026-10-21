import QtQuick
import QtQuick.Controls
import QtQuick.Layouts 1.15
import QtQuick.Controls.Material
import Qt.labs.qmlmodels
import QtQuick.Dialogs

Item {
    id: sqlRoot
    Layout.margins: 10
    Layout.alignment: Qt.AlignCenter
    Layout.preferredWidth: 400 //Math.max(parent.width * 0.2, 400) //RelativeSize
    Layout.preferredHeight: 170 //Math.max(parent.height * 0.15, 170) //RelativeSize

    // Property so we can set text when adding this to a layout
    property string itemColour: "#e0e0e0"
    property string itemBorderColour: "#dd0000"
    property string subItemBorderColour: "#0400ff"

    Rectangle {
        id: box
        anchors.fill: parent
        color: itemColour
        radius: 8
        border.color: itemBorderColour
        width: parent.width
        height: parent.height
        Layout.margins: 10

        Rectangle{
            anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
            color: itemColour
            height: title.height/2
            width: title.width*1.1
            Text {
                id: title
                anchors.verticalCenter: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                text: qsTr("SQL Controller")
                font.pixelSize: 18
                font.weight: Font.DemiBold
                z: 1
            }
        }
    }

    Rectangle {
        id: boxTop
        anchors.top: parent.top; anchors.right: parent.right
        anchors.rightMargin: 5; anchors.topMargin: 10
        height: parent.height/3
        width: parent.width*2/3 -10
        color: itemColour
        radius: 8
        border.color: subItemBorderColour

        Row {
            anchors.top: parent.top; anchors.left: parent.left
            anchors.topMargin: 5; anchors.leftMargin: 5
            Text { text: qsTr("Manifest Sync State: "); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
            
            // Manifest Synced Indicator
            Rectangle {
                anchors.verticalCenter: parent.verticalCenter
                width: 20
                height: width
                radius: width/2
                color: if (backend.isSynced) {
                    "green"
                } else if (dmController.sqlLoaded && dmController.fileLoaded) {
                    "red"
                } else {
                    "orange"
                }
            }
        }
        CheckBox {
            id: fastSyncCheck
            anchors.top: parent.top; anchors.right: parent.right
            anchors.topMargin: -10; anchors.rightMargin: 5
            checked: true
            text: qsTr("Fast Sync")
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 1000
                dim: true
                text: qsTr("Speed up syncing by using a partial validation method (only checking if files stated to exist, do), and by not deleting SQL data before updating database.")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }
        }
        Button {
            text: qsTr("Sync Database")
            enabled: dmController.sqlConnected
            anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
            onClicked: {
                // Just do sync if fast (safe), otherwise confirm
                if (fastSyncCheck.checked) {
                    console.log("Sync database (fastSync).")
                    backend._SQLSync(fastSyncCheck.checked)
                } else {
                    // Dialog title/message
                    globalConfirm.title = "Sync Database"
                    globalConfirm.message = "Are you sure you want to use full (non-fast) sync? Saved data that is not also saved in direct files may be lost."
                    
                    // onConfirm does the action
                    globalConfirm.onConfirm = function() {
                        console.log("Sync database (not fastSync).")
                        backend._SQLSync(fastSyncCheck.checked)
                    }

                    // Open the dialog
                    globalConfirm.open()
                }
                
            }
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 1000
                dim: true
                text: qsTr("Synchronises data saved as direct files into database (only Direct --> Database). Caution: Disabling fast sync may cause loss of data.")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }

            Material.background: if (fastSyncCheck.checked) {
                Material.basic
            } else {
                Material.Red
            }
            Material.foreground: if (fastSyncCheck.checked) {
                "black"
            } else {
                "white"
            }
            Material.elevation: if (fastSyncCheck.checked) {
                2
            } else {
                5
            }

            width: Math.max(box.width * 0.15, 120) //RelativeSize
            height: Math.max(box.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
            leftPadding: width*0.08 //RelativeSize
            rightPadding: width*0.08 //RelativeSize
        }
    }

    Rectangle {
        id: boxMid
        anchors.top: boxTop.bottom; anchors.right: parent.right
        anchors.rightMargin: 5; anchors.topMargin: 10
        height: parent.height/3
        width: parent.width*2/3 -10
        color: itemColour
        radius: 8
        border.color: subItemBorderColour

        CheckBox {
            id: deepRepairCheck
            anchors.top: parent.top; anchors.right: parent.right
            anchors.topMargin: -10; anchors.rightMargin: 5
            checked: false
            text: qsTr("Deep Repair")
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 1000
                dim: true
                text: qsTr("Conduct deep repair by nuking the database, and rebuilding the tables and syncing data from scratch. Caution: Resets database (on top of deleting existing data).")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }
        }

        CheckBox {
            id: dataRepairCheck
            anchors.top: parent.top; anchors.left: parent.left
            anchors.topMargin: -10; anchors.leftMargin: 5
            checked: false
            text: qsTr("Data Repair")
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 1000
                dim: true
                text: qsTr("Repair the data by syncing data from direct storage into database. If using deep repair, fast sync option is not used. Caution: Enabling data repair deletes all data in database.")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }
        }

        Button {
            anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
            text: qsTr("Repair Database")
            enabled: dmController.sqlConnected
            onClicked: {
                if (deepRepairCheck.checked) {
                    // Dialog title/message
                    globalDestructConfirm.title = "Repair Database"
                    globalDestructConfirm.message = "Are you sure you want to repair the database? All data in the database will be lost!"
                    globalDestructConfirm.theme = "extreme"

                    // onConfirm does the action
                    globalDestructConfirm.onConfirm = function() {
                        console.log("Repairing database.")
                        backend._SQLRepair(self, deepRepairCheck.checked, dataRepairCheck.checked)
                    }

                    // Open the dialog
                    globalDestructConfirm.open()
                } else if (dataRepairCheck.checked) {
                    // Dialog title/message
                    globalDestructConfirm.title = "Repair Database"
                    globalDestructConfirm.message = "Are you sure you want to repair the database? Data in the database will be lost!"

                    // onConfirm does the action
                    globalDestructConfirm.onConfirm = function() {
                        console.log("Repairing database.")
                        backend._SQLRepair(self, deepRepairCheck.checked, dataRepairCheck.checked)
                    }

                    // Open the dialog
                    globalDestructConfirm.open()

                } else {
                    backend._SQLRepair(self, deepRepairCheck.checked, dataRepairCheck.checked)
                }
                
            }

            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 600
                dim: true
                text: qsTr("Repair database, fixing keys (and tables if needed, will require re-syncing of data if tables require repair). Caution: Integrity of database-stored data may be damaged if table relation is damaged, or if Data Repair or Deep Repair are enabled.")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }

            Material.background: if (deepRepairCheck.checked) {
                Material.Yellow
            } else if (dataRepairCheck.checked) {
                Material.Red
            } else {
                Material.basic
            }
            Material.foreground: if (deepRepairCheck.checked) {
                "red"
            } else if (dataRepairCheck.checked) {
                "white"
            } else {
                "black"
            }
            Material.elevation: if (deepRepairCheck.checked) {
                10
            } else if (dataRepairCheck.checked) {
                5
            } else {
                2
            }

            width: Math.max(box.width * 0.15, 120) //RelativeSize
            height: Math.max(box.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
            leftPadding: width*0.08 //RelativeSize
            rightPadding: width*0.08 //RelativeSize
        }

    }

    Rectangle {
        id: boxLeft
        anchors.top: parent.top; anchors.left: parent.left
        anchors.leftMargin: 5; anchors.topMargin: 10
        height: parent.height/3
        width: parent.width*1/3 -5
        color: itemColour
        radius: 8
        border.color: subItemBorderColour

        Button {
            anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
            text: qsTr("Setup Database")
            enabled: dmController.sqlConnected
            onClicked: {
                // Dialog title/message
                globalDestructConfirm.title = "Setup Database"
                globalDestructConfirm.message = "Are you sure you want to setup the database? All existing data (if already set up) will be lost."
                
                // onConfirm does the action
                globalDestructConfirm.onConfirm = function() {
                    console.log("Setting up database.")
                    backend._SQLSetup()
                }

                // Open the dialog
                globalDestructConfirm.open()
            }
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 600
                dim: true
                text: qsTr("Setup SQL database for ArcaneQuant use. Warning: Database-stored data will be deleted.")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }

            Material.background: Material.Red
            Material.foreground: "white"
            Material.elevation: 5

            width: Math.max(box.width * 0.15, 120) //RelativeSize
            height: Math.max(box.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
            leftPadding: width*0.08 //RelativeSize
            rightPadding: width*0.08 //RelativeSize
        }

        Button {
            anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
            text: qsTr("Clear Database")
            enabled: dmController.sqlConnected
            onClicked: {
                // Dialog title/message
                globalDestructConfirm.title = "Clear Database"
                globalDestructConfirm.message = "Are you sure you want to clear the database? All data will be lost."
                
                // onConfirm does the action
                globalDestructConfirm.onConfirm = function() {
                    console.log("Clearing database.")
                    backend._SQLClear()
                }

                // Open the dialog
                globalDestructConfirm.open()
            }
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 600
                dim: true
                text: qsTr("Clears database of all data rows (stock prices, tickers etc.). Use with caution.")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }
            

            Material.background: Material.Red
            Material.foreground: "white"
            Material.elevation: 5

            width: Math.max(box.width * 0.15, 120) //RelativeSize
            height: Math.max(box.height * 0.15, 30) //RelativeSize // TODO: ADD COLOUR
            leftPadding: width*0.08 //RelativeSize
            rightPadding: width*0.08 //RelativeSize
        }

    }
    
    Row {
        id: botButtonRow
        anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
        anchors.leftMargin: 10; anchors.rightMargin: 10
        anchors.topMargin: 5; anchors.bottomMargin: 5
        spacing: parent.width * 0.2-70 //RelativeSize
        
        Button {
            id: connectButton
            text: qsTr("Connect to SQL")
            enabled: backend.hasDataManifest
            onClicked: {
                sqlLoadDialog.open()
            }
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 1000
                dim: true
                text: qsTr("Connect DataManifest to SQL database via provided .env file.")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }

            width: Math.max(box.width * 0.15, 120) //RelativeSize
            height: Math.max(box.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
            leftPadding: width*0.08 //RelativeSize
            rightPadding: width*0.08 //RelativeSize
        }

        Button {
            id: savebutton
            text: qsTr("Execute SQL")
            enabled: dmController.sqlConnected
            property string saveTo: 'database'
            onClicked: {
                
            }
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 1000
                dim: true
                text: qsTr("Executes given SQL queries as command-line inputs")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }

            //Material.theme: Material.Dark
            Material.background: "#000000"
            Material.foreground: "white"

            width: Math.max(box.width * 0.15, 120) //RelativeSize
            height: Math.max(box.height * 0.15, 30) //RelativeSize
            leftPadding: width*0.08 //RelativeSize
            rightPadding: width*0.08 //RelativeSize
        }
        Button {
            id: nukeButton
            text: qsTr("NUKE Database")
            enabled: dmController.sqlConnected
            property string loadFrom: 'database'
            onClicked: {
                // Dialog title/message
                globalDestructConfirm.title = "NUKING DATABASE"
                globalDestructConfirm.message = "Are you sure you want to nuke the database? All data and views on the database will be lost."
                globalDestructConfirm.theme = "extreme"

                // onConfirm does the action
                globalDestructConfirm.onConfirm = function() {
                    console.log("Nuking database!")
                    backend._SQLNuke()
                }

                // Open the dialog
                globalDestructConfirm.open()
            }
            // Tooltip settings
            ToolTip {
                parent: parent
                visible: parent.hovered
                delay: 50
                dim: true
                text: qsTr("Wipes all data and tables/views from SQL database. USE WITH EXTREME CAUTION!")

                x: (parent.width-width)/2
                y: parent.height
                width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
            }
            
            Material.background: Material.Yellow
            Material.foreground: "red"
            Material.elevation: 10

            width: Math.max(box.width * 0.15, 120) //RelativeSize
            height: Math.max(box.height * 0.15, 30) //RelativeSize
            leftPadding: width*0.08 //RelativeSize
            rightPadding: width*0.08 //RelativeSize
        }
        
    }

    // FileDialog for SQL file load
    FileDialog {
        id: sqlLoadDialog
        currentFolder: backend.current_url_env
        nameFilters: ["Env files (*.env)", "All supported types (*.env)", "All files (*)"]
        onAccepted: {
            backend.current_url_env = sqlLoadDialog.currentFolder

            // Separate folder and file (name only) (let is local scope)
            if (sqlLoadDialog.selectedFile) { // Avoid undefined errors
                let filePath = sqlLoadDialog.selectedFile.toString()
                
                // /[]/ is the JS regex handle notation, handles both / and \ and first pops out last element (file)
                let fileName = filePath.split(/[/\\]/).pop().split('.')
                // Remove extension
                let fileExt  = fileName.pop()
                // Take out fileName from array of len 1
                fileName = fileName.pop()
                
                // Run _connectSQL //
                backend._connectSQL(fileName, sqlLoadDialog.selectedFolder)
                dmController.sqlConnected = true
                dmController.sqlSourceLabel = qsTr(backend.strEngine)

            } else {
                console.log("No file selected")
            }
        }
    }

}