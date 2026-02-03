import QtQuick 2.6
import QtQuick.Controls 2.5
import QtQuick.Layouts 1.15
import QtQuick.Controls.Material
import QtQuick.Dialogs

// Note I use //RelativeSize as a market to distinguish percentage sizes so I can modify easily

Item {
    id: loader
    Layout.margins: 10
    Layout.alignment: Qt.AlignCenter
    Layout.preferredWidth: 400 //Math.max(parent.width * 0.2, 400) //RelativeSize
    Layout.preferredHeight: 170 //Math.max(parent.height * 0.15, 170) //RelativeSize

    // Property so we can set text when adding this to a layout
    property string itemText: qsTr("Load DataManifest to begin")
    property string itemColour: "#e0e0e0"
    property string itemBorderColour: "#dd0000"
    property bool sqlConnected: false
    property bool fileLinked: false

    property alias sqlSourceLabel: sqlLabel.text
    property alias fileSourceLabel: dirLabel.text
    property bool sqlLoaded: false
    property bool fileLoaded: false

    function canSaveLoad(option) {
        if (option == 'Direct') {
            return fileLinked
        } else if (option == 'Database') {
            return sqlConnected
        } else if (option == 'Both') {
            return fileLinked && sqlConnected
        }
        return false // Fallback in case wrong input
    }

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
                text: qsTr("DataManifest Controller")
                font.pixelSize: 18
                font.weight: Font.DemiBold
                z: 1
            }
        }

        Row {
            anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
            anchors.topMargin: 15
            spacing: 5
            Text { text: qsTr("Save setting:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
            // Combo box for selecting save options
            ComboBox {
                id: saveops
                model: [qsTr('Direct'), qsTr('Database'), qsTr('Both')]
                width: 95
                height: 30
                currentIndex: 0
                rightPadding: 30
                leftPadding: -5
                anchors.verticalCenter: parent.verticalCenter
                // TODO: ADD OPERATION HERE
            }

            Item { height:1; width: 30 } // Spacer item
            
            Text { id:loadtext; text: qsTr("Load setting:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
            // Combo box for selecting load options
            ComboBox {
                id: loadops
                model: [qsTr('Direct'), qsTr('Database')]
                width: 95
                height: 30
                currentIndex: 0
                rightPadding: 30
                leftPadding: -5
                anchors.verticalCenter: parent.verticalCenter
                // TODO: ADD OPERATION HERE
            }
        }
        Row {
            id: sqlSourceRow
            anchors.bottom: fileSourceRow.top; anchors.left: parent.left
            anchors.bottomMargin: 7
            anchors.leftMargin: 15
            spacing: 10

            // SQL Loaded Indicator
            Rectangle {
                anchors.verticalCenter: parent.verticalCenter
                width: 20
                height: width
                radius: width/2
                color: if (dmController.sqlConnected) {
                    "green"
                } else {
                    "red"
                }
            }
            // Text showing connected SQL engine/source
            Text {
                id: sqlSourceLabel
                anchors.verticalCenter: parent.verticalCenter
                text: qsTr("SQL Engine: ")
                font.pixelSize: 12
            }
            Text {
                id: sqlLabel
                anchors.verticalCenter: parent.verticalCenter
                text: loader.itemText
                font.pixelSize: 12
                width: box.width*0.65 //RelativeSize
                elide: Text.ElideLeft
            }
        }
        Row {
            id: fileSourceRow
            anchors.verticalCenter: parent.verticalCenter; anchors.left: parent.left
            anchors.bottomMargin: 8
            anchors.leftMargin: 15
            spacing: 10

            // Manifest File Linked Indicator
            Rectangle {
                anchors.verticalCenter: parent.verticalCenter
                width: 20
                height: width
                radius: width/2
                color: if (dmController.fileLinked) {
                    "green"
                } else {
                    "red"
                }
            }
            // Text showing linked file directory/source
            Text {
                id: fileSourceLabel
                anchors.verticalCenter: parent.verticalCenter
                text: qsTr("File Source: ")
                font.pixelSize: 12
            }
            Text {
                id: dirLabel
                anchors.verticalCenter: parent.verticalCenter
                text: loader.itemText
                font.pixelSize: 12
                width: box.width*0.65 //RelativeSize
                elide: Text.ElideLeft
            }
        }
        Row {
            anchors.bottom: saveRow.top; anchors.horizontalCenter: saveRow.horizontalCenter
            anchors.leftMargin: 10; anchors.rightMargin: 10
            anchors.topMargin: 5; anchors.bottomMargin: 5
            spacing: parent.width * 0.2-70 //RelativeSize

            
            Item {
                height: savebutton.height; width: savebutton.width
                Text {
                    anchors.centerIn: parent
                    text: qsTr(`Current Link: 
` + backend.currentAssignment)
                }
                
            }

            Button {
                text: qsTr("Validate")
                enabled: backend.hasDataManifest && fileLinked // Only if file linked as that is the only validation option right now
                onClicked: dirLabel.text = qsTr("Clicked!")
                // Tooltip settings
                ToolTip {
                    parent: parent
                    visible: parent.hovered
                    delay: 1000
                    dim: true
                    text: qsTr("Validates by checking if data indicated to exist in DataManifest exists (and vice versa).")

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
                text: qsTr("Delete")
                enabled: backend.hasDataManifest
                onClicked: {
                    backend._delManifest()
                    loader.sqlConnected = false
                    loader.sqlLoaded = false
                    loader.fileLinked = false
                    loader.fileLoaded = false
                    sqlSourceLabel.text = qsTr("SQL Engine: ")
                    sqlLabel.text = qsTr("Deleted. Load DataManifest to begin.")
                    fileSourceLabel.text = qsTr("File Source: ")
                    dirLabel.text = qsTr("Deleted. Load DataManifest to begin.")
                }
                // Tooltip settings
                ToolTip {
                    parent: parent
                    visible: parent.hovered
                    delay: 600
                    dim: true
                    text: qsTr("Closes session, deleting existing manifest. Any unsaved changes are lost.")

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
            
        }
        Row {
            id: saveRow
            anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
            anchors.leftMargin: 10; anchors.rightMargin: 10
            anchors.topMargin: 5; anchors.bottomMargin: 5
            spacing: parent.width * 0.2-70 //RelativeSize
            Button {
                id: newbutton
                text: qsTr("New DataManifest")
                onClicked: {
                    backend._newManifest()
                    loader.sqlConnected = false
                    loader.sqlLoaded = false
                    loader.fileLinked = false
                    loader.fileLoaded = false
                    sqlSourceLabel.text = qsTr("SQL Engine: ")
                    sqlLabel.text = qsTr("DataManifest Initialised")
                    fileSourceLabel.text = qsTr("File Source: ")
                    dirLabel.text = qsTr("DataManifest Initialised")
                }
                // Tooltip settings
                ToolTip {
                    parent: parent
                    visible: parent.hovered
                    delay: 1000
                    dim: true
                    text: qsTr("Start a new session by making a new DataManifest. Existing unsaved data is lost.")

                    x: (parent.width-width)/2
                    y: parent.height
                    width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
                }
                
                Material.background: Material.Green
                Material.foreground: "white"
                
                width: Math.max(box.width * 0.15, 120) //RelativeSize
                height: Math.max(box.height * 0.15, 30) //RelativeSize // TODO: ADD COLOUR
                leftPadding: width*0.08 //RelativeSize
                rightPadding: width*0.08 //RelativeSize
            }

            Button {
                id: savebutton
                text: qsTr("Save")
                // If connection to file/engine and relevant option selected (if both, need both connection), enabled
                enabled: (backend.hasDataManifest) && loader.canSaveLoad(saveops.currentValue)
                property string saveTo: 'database'
                onClicked: {
                    if (saveops.currentValue == 'Direct') { // Save into file only
                        saveTo = 'direct'
                        
                        manifestSaveDialog.open() // Save manifest on accept in dialog
                    } else if (saveops.currentValue == 'Both') { // Save into both
                        data
                        
                    } else { // Database save
                        // If no engine
                        //backend._loadManifest(saveTo, "", "") //CHECK OUTPUT
                    }
                }

                width: Math.max(box.width * 0.15, 120) //RelativeSize
                height: Math.max(box.height * 0.15, 30) //RelativeSize
                leftPadding: width*0.08 //RelativeSize
                rightPadding: width*0.08 //RelativeSize
            }
            Button {
                id: loadbutton
                text: qsTr("Load")
                enabled: backend.hasDataManifest
                property string loadFrom: 'database'
                onClicked: {
                    if (loadops.currentValue == 'Direct') { // Load from file
                        loadFrom = 'direct'
                        manifestLoadDialog.open() // Load manifest on accept in dialog
                    } else { // Database load
                        loadFrom = 'database'
                        backend._loadManifest(loadFrom, "", "")
                        loader.sqlLoaded = true
                    }
                    
                    
                }
                width: Math.max(box.width * 0.15, 120) //RelativeSize
                height: Math.max(box.height * 0.15, 30) //RelativeSize
                leftPadding: width*0.08 //RelativeSize
                rightPadding: width*0.08 //RelativeSize
            }
            
        }
    }

    // FileDialogs for loading/saving
    // FileDialog for DataManifest load (direct)
    FileDialog {
        id: manifestLoadDialog
        currentFolder: backend.current_url_json
        nameFilters: ["JSON files (*.json)", "Text files (*.txt)", "All supported types (*.json; *.txt)", "All files (*)"]
        onAccepted: {
            backend.current_url_json = manifestLoadDialog.currentFolder
            let folderPath = backend.current_path_json + "/" // Directory

            // Getting file name
            let filePath = manifestLoadDialog.selectedFile.toString()
                
            // /[]/ is the JS regex handle notation, handles both / and \ and first pops out last element (file)
            let fileName = filePath.split(/[/\\]/).pop().split('.')
            // Remove extension
            let fileExt  = fileName.pop()
            // Take out fileName from array of len 1
            fileName = fileName.pop()

            fileSourceLabel.text = qsTr("File Source: ")
            dirLabel.text = qsTr(manifestLoadDialog.selectedFile.toString())
            
            backend._loadManifest(loadbutton.loadFrom, folderPath, fileName)
            loader.fileLinked = true
            loader.fileLoaded = true
        }
    }

    // FileDialog for DataManifest save
    FileDialog {
        id: manifestSaveDialog
        fileMode: FileDialog.SaveFile
        currentFolder: backend.current_url_json
        nameFilters: ["JSON files (*.json)", "All files (*)"]
        onAccepted: {
            backend.current_url_json = manifestSaveDialog.currentFolder
            let folderPath = backend.current_path_json + "/" // Directory

            // Getting file name
            let filePath = manifestSaveDialog.selectedFile.toString()
                
            // /[]/ is the JS regex handle notation, handles both / and \ and first pops out last element (file)
            let fileName = filePath.split(/[/\\]/).pop().split('.')
            // Remove extension
            let fileExt  = fileName.pop()
            // Take out fileName from array of len 1
            fileName = fileName.pop()

            dirLabel.text = qsTr(manifestSaveDialog.selectedFile.toString())
            
            //backend._loadManifest(loadbutton.loadFrom, folderPath, fileName)
        }
    }
    
}