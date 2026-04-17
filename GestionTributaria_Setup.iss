; ============================================================
;  Inno Setup Script — Sistema de Gestión Tributaria v1.0.0
;  Universidad Santiago de Cali — Ingeniería de Sistemas
;  Autor: Alvaro Jose Mosquera Morales
; ============================================================

#define AppName      "Gestión Tributaria"
#define AppVersion   "1.0.0"
#define AppPublisher "Universidad Santiago de Cali"
#define AppURL       "https://github.com/AlvaroJMosquera/gestion-tributaria"
#define AppExeName   "GestionTributaria.exe"
#define AppYear      "2026"

[Setup]
DiskSpanning=yes
; --- Identificador único de la aplicación (no cambiar en actualizaciones)
AppId={{A7F2C3D4-E891-4B2A-9F3E-1D2C4B5A6E7F}

; --- Información de la aplicación
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
AppUpdatesURL={#AppURL}
AppCopyright=Copyright © {#AppYear} {#AppPublisher}

; --- Directorio de instalación
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes

; --- Archivo de licencia (se muestra antes de instalar)
LicenseFile=LICENSE.rtf

; --- Imagen de bienvenida lateral (164x314 px, BMP o GIF)
; WizardImageFile=wizard_image.bmp
; WizardSmallImageFile=wizard_small.bmp

; --- Salida del instalador
OutputDir=Output
OutputBaseFilename=GestionTributaria_Setup_v{#AppVersion}
SetupIconFile=icon.ico

; --- Compresión
Compression=lzma2/ultra64
SolidCompression=yes
LZMAUseSeparateProcess=yes

; --- Arquitectura
ArchitecturesInstallIn64BitMode=x64compatible

; --- Apariencia
WizardStyle=modern
WizardResizable=no

; --- Privilegios
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog

; --- Metadatos para "Agregar o quitar programas"
UninstallDisplayName={#AppName} {#AppVersion}
UninstallDisplayIcon={app}\{#AppExeName}
VersionInfoVersion={#AppVersion}
VersionInfoCompany={#AppPublisher}
VersionInfoDescription=Sistema de facturación electrónica asistida por IA
VersionInfoProductName={#AppName}
VersionInfoProductVersion={#AppVersion}
VersionInfoCopyright=Copyright © {#AppYear} {#AppPublisher}

; --- No crear entrada en menú Inicio por defecto (lo manejamos en [Icons])
DisableDirPage=no

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"

[CustomMessages]
spanish.WelcomeLabel1=Bienvenido al instalador de%n{#AppName}
spanish.WelcomeLabel2=Este asistente instalará {#AppName} {#AppVersion} en su equipo.%n%nDesarrollado por Alvaro Jose Mosquera Morales%nDirector: Jair Enrique Sanclemente Castro%n%nUniversidad Santiago de Cali — {#AppYear}%n%nSe recomienda cerrar todas las aplicaciones antes de continuar.

[Tasks]
Name: "desktopicon";    Description: "Crear acceso directo en el &Escritorio";       GroupDescription: "Accesos directos:"
Name: "startmenuicon";  Description: "Crear acceso directo en el &Menú Inicio";      GroupDescription: "Accesos directos:"

[Files]
; Ejecutable principal
Source: "dist\{#AppExeName}"; DestDir: "{app}"; Flags: ignoreversion

; Archivos del motor IA (Ollama standalone y modelos locales)
Source: "installer_assets\*"; DestDir: "{app}\installer_assets"; Flags: ignoreversion recursesubdirs

; Si en el futuro agregas archivos adicionales, añádelos aquí:
; Source: "dist\.env";          DestDir: "{app}"; Flags: ignoreversion
; Source: "dist\config\*";      DestDir: "{app}\config"; Flags: ignoreversion recursesubdirs

[Icons]
; Acceso directo en Menú Inicio
Name: "{group}\{#AppName}";              Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\{#AppExeName}"; Tasks: startmenuicon
Name: "{group}\Desinstalar {#AppName}";  Filename: "{uninstallexe}";                                           Tasks: startmenuicon

; Acceso directo en Escritorio
Name: "{autodesktop}\{#AppName}";        Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Run]
; Opción para ejecutar la app al terminar el instalador
Filename: "{app}\{#AppExeName}"; Description: "Iniciar {#AppName} ahora"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; Limpiar archivos generados en tiempo de ejecución al desinstalar
Type: filesandordirs; Name: "{app}\logs"
Type: filesandordirs; Name: "{app}\__pycache__"

[Code]
// ── Verificación de versión previa instalada ──────────────────────────
function InitializeSetup(): Boolean;
var
  OldVersion: String;
  UninstallString: String;
  ResultCode: Integer;
begin
  Result := True;

  if RegQueryStringValue(HKLM, 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{A7F2C3D4-E891-4B2A-9F3E-1D2C4B5A6E7F}_is1',
    'DisplayVersion', OldVersion) or
     RegQueryStringValue(HKCU, 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{A7F2C3D4-E891-4B2A-9F3E-1D2C4B5A6E7F}_is1',
    'DisplayVersion', OldVersion) then
  begin
    if MsgBox('Se encontró una versión anterior (' + OldVersion + ') instalada.' + #13#10 +
              'Se desinstalará antes de continuar. ¿Desea continuar?',
              mbConfirmation, MB_YESNO) = IDYES then
    begin
      if RegQueryStringValue(HKLM, 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{A7F2C3D4-E891-4B2A-9F3E-1D2C4B5A6E7F}_is1',
        'UninstallString', UninstallString) or
         RegQueryStringValue(HKCU, 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{A7F2C3D4-E891-4B2A-9F3E-1D2C4B5A6E7F}_is1',
        'UninstallString', UninstallString) then
      begin
        Exec(RemoveQuotes(UninstallString), '/SILENT', '', SW_SHOW, ewWaitUntilTerminated, ResultCode);
      end;
    end
    else
      Result := False;
  end;
end;
