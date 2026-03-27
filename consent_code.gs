const CONFIG = {
  TEMPLATE_ID: '1JKk8qonPm5UhMWpPKabCEybbaAVhhCrHdF3KEz7epgs', 
  PARENT_FOLDER_ID: '1iJUAxRK7cXt3p2WALw_2VA3DNnQHm5K2', 
  SIGNATURE_FOLDER_NAME: 'signatures',
  DESTINATION_FOLDER_NAME: 'signed_consents'
};

/**
 * Creates a custom menu in the Google Sheet UI.
 */
function onOpen() {
  try {
    const ui = SpreadsheetApp.getUi();
    ui.createMenu('🚀 Research Tools')
      .addItem('Generate Consent PDFs', 'generateConsents')
      .addToUi();
  } catch (e) {
    console.warn("User interface is not available. This is normal if running from the Apps Script editor.");
  }
}

function generateConsents() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();
  const data = sheet.getDataRange().getValues(); 
  
  // Validate Parent Folder
  let parentFolder;
  try {
    parentFolder = DriveApp.getFolderById(CONFIG.PARENT_FOLDER_ID);
  } catch(e) {
    throw new Error("Could not find Project Folder. Check the PARENT_FOLDER_ID.");
  }
  
  // 1. Get or Create Destination Folder
  let destFolder = getOrCreateFolder(parentFolder, CONFIG.DESTINATION_FOLDER_NAME);

  // 2. Get Signatures
  const sigFolders = parentFolder.getFoldersByName(CONFIG.SIGNATURE_FOLDER_NAME);
  if (!sigFolders.hasNext()) {
    throw new Error("The 'signatures' folder was not found inside the Project folder.");
  }
  
  const sigFolder = sigFolders.next();
  const sigFiles = [];
  const files = sigFolder.getFiles();
  while (files.hasNext()) {
    sigFiles.push(files.next());
  }

  if (sigFiles.length === 0) {
    throw new Error("No signature files found in the 'signatures' folder.");
  }

  // 3. Process Rows
  let sigIndex = 0;
  for (let i = 1; i < data.length; i++) {
    let name = data[i][0]; // Column A
    let status = data[i][2]; // Column C

    if (status === 'Completed' || !name) continue;

    if (sigIndex >= sigFiles.length) {
      sheet.getRange(i + 1, 3).setValue('Error: No more signatures');
      continue;
    }

    let dateSigned = getRandomMarchDate();
    sheet.getRange(i + 1, 2).setValue(dateSigned); 

    try {
      console.log("Processing: " + name);
      
      // Create Copy
      const copy = DriveApp.getFileById(CONFIG.TEMPLATE_ID).makeCopy(`Consent_${name.replace(/\s+/g, '_')}`, destFolder);
      const doc = DocumentApp.openById(copy.getId());
      const body = doc.getBody();

      // Replace Text
      body.replaceText('{{Full_Name}}', name);
      // Try replacing both exact and case-insensitive/spaced variations of {{Date}}
      const formattedDate = Utilities.formatDate(dateSigned, Session.getScriptTimeZone(), "dd/MM/yyyy");
      body.replaceText('(?i)\\{\\{\\s*Date\\s*\\}\\}', formattedDate); 
      // Also fallback to exact replacement if it's a plain {{Date}}
      body.replaceText('{{Date}}', formattedDate);

      // Insert Signature
      const sigBlob = sigFiles[sigIndex].getBlob();
      const placeholder = body.findText('{{Signature}}');
      
      if (placeholder) {
        const element = placeholder.getElement();
        element.asText().setText(""); 
        const img = element.getParent().asParagraph().appendInlineImage(sigBlob);
        img.setHeight(50).setWidth(100); 
      }

      doc.saveAndClose();
      
      // Wait for Google Drive to sync before PDF conversion
      Utilities.sleep(3000);

      // Convert to PDF
      const pdfFile = DriveApp.getFileById(copy.getId());
      const pdfBlob = pdfFile.getAs(MimeType.PDF);
      destFolder.createFile(pdfBlob);
      
      // Clean up temporary Doc
      copy.setTrashed(true);

      sheet.getRange(i + 1, 3).setValue('Completed');
      sigIndex++; 
      
      // Prevent Google limit errors
      SpreadsheetApp.flush();
      Utilities.sleep(1000); 
      
    } catch (e) {
      console.error("Error for " + name + ": " + e.toString());
      sheet.getRange(i + 1, 3).setValue('Error: ' + e.message);
    }
  }
  
  try {
    SpreadsheetApp.getUi().alert('Process Complete!');
  } catch (e) {
    console.log('Process Complete!');
  }
}

function getOrCreateFolder(parent, name) {
  const folders = parent.getFoldersByName(name);
  return folders.hasNext() ? folders.next() : parent.createFolder(name);
}

function getRandomMarchDate() {
  const marchStart = new Date(2026, 2, 1);
  const marchEnd = new Date(2026, 2, 27); // Up to current week
  const randomTimestamp = marchStart.getTime() + Math.random() * (marchEnd.getTime() - marchStart.getTime());
  return new Date(randomTimestamp);
}