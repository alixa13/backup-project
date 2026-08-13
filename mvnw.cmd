@ECHO OFF
:: ----------------------------------------------------------------------------
:: Maven Wrapper startup script (Windows).
::
:: Delegates to .mvn\wrapper\maven-wrapper.jar, downloading it first if it is
:: not already present, using the URL pinned in
:: .mvn\wrapper\maven-wrapper.properties. Requires network access on first run.
:: ----------------------------------------------------------------------------

SETLOCAL

SET BASEDIR=%~dp0
SET WRAPPER_JAR=%BASEDIR%.mvn\wrapper\maven-wrapper.jar
SET WRAPPER_PROPERTIES=%BASEDIR%.mvn\wrapper\maven-wrapper.properties
SET WRAPPER_LAUNCHER=org.apache.maven.wrapper.MavenWrapperMain

IF NOT EXIST "%WRAPPER_JAR%" (
  FOR /F "tokens=1,* delims==" %%A IN ('findstr /R "^wrapperUrl=" "%WRAPPER_PROPERTIES%"') DO SET WRAPPER_URL=%%B
  ECHO Downloading Maven Wrapper from: %WRAPPER_URL%
  powershell -Command "Invoke-WebRequest -Uri '%WRAPPER_URL%' -OutFile '%WRAPPER_JAR%'"
)

IF NOT "%JAVA_HOME%"=="" (
  SET JAVA_CMD=%JAVA_HOME%\bin\java.exe
) ELSE (
  SET JAVA_CMD=java.exe
)

"%JAVA_CMD%" -classpath "%WRAPPER_JAR%" "-Dmaven.multiModuleProjectDirectory=%BASEDIR%" %WRAPPER_LAUNCHER% %*

ENDLOCAL
