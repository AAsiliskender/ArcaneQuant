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
                text: qsTr("DataManifest Settings")
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
            anchors.centerIn: parent
            anchors.bottomMargin: 10
            spacing: 10

            Text {
                id: sourceLabel
                text: qsTr("No Source: ")
                font.pixelSize: 12
            }
            Text {
                id: dirLabel
                text: loader.itemText
                font.pixelSize: 12
                width: box.width*0.75 //RelativeSize
                elide: Text.ElideLeft
            }
        }
        Row {
            anchors.bottom: saveRow.top; anchors.horizontalCenter: saveRow.horizontalCenter
            anchors.leftMargin: 10; anchors.rightMargin: 10
            anchors.topMargin: 5; anchors.bottomMargin: 5
            spacing: parent.width * 0.2-70 //RelativeSize

            Button {
                text: qsTr("Connect to SQL")
                enabled: backend.hasDataManifest
                onClicked: {
                    sqlLoadDialog.open()
                }
                width: Math.max(box.width * 0.15, 120) //RelativeSize
                height: Math.max(box.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
                leftPadding: width*0.08 //RelativeSize
                rightPadding: width*0.08 //RelativeSize
            }

            Button {
                text: qsTr("Validate DataManifest")
                enabled: backend.hasDataManifest
                onClicked: dirLabel.text = qsTr("Clicked!")
                width: Math.max(box.width * 0.15, 120) //RelativeSize
                height: Math.max(box.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
                leftPadding: width*0.08 //RelativeSize
                rightPadding: width*0.08 //RelativeSize
            }

            Button {
                text: qsTr("Delete DataManifest")
                enabled: backend.hasDataManifest
                onClicked: {
                    backend._delManifest()
                    sourceLabel.text = qsTr("Deleted: ")
                    dirLabel.text = qsTr("No DataManifest. Load DataManifest to begin.")
                }
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
                    sourceLabel.text = qsTr("Initialised: ")
                    dirLabel.text = qsTr("New DataManifest")
                }
                width: Math.max(box.width * 0.15, 120) //RelativeSize
                height: Math.max(box.height * 0.15, 30) //RelativeSize // TODO: ADD COLOUR
                leftPadding: width*0.08 //RelativeSize
                rightPadding: width*0.08 //RelativeSize
            }

            Button {
                id: savebutton
                text: qsTr("Save")
                enabled: backend.hasDataManifest
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
                        backend._loadManifest(loadFrom, "", "") //CHECK OUTPUT
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
                backend._connectSQL(fileName,sqlLoadDialog.selectedFolder)
                loader.sqlConnected = true
                sourceLabel.text = qsTr("Database: ")
                dirLabel.text = qsTr(backend.strEngine)

            } else {
                console.log("No file selected")
            }
        }
    }

    // FileDialog for DataManifest load
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

            sourceLabel.text = qsTr("File: ")
            dirLabel.text = qsTr(manifestLoadDialog.selectedFile.toString())
            
            backend._loadManifest(loadbutton.loadFrom, folderPath, fileName)
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

            sourceLabel.text = qsTr("File: ")
            dirLabel.text = qsTr(manifestSaveDialog.selectedFile.toString())
            
            //backend._loadManifest(loadbutton.loadFrom, folderPath, fileName)
        }
    }
    
}