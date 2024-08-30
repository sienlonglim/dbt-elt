# Replace 'path\to\.env' with the actual path to your .env file
$envFile = ".env"

# Read and export each line from the .env file
Get-Content $envFile | ForEach-Object {
    # Skip empty lines or lines starting with #
    if ($_ -and $_ -notmatch '^\s*#') {
        $parts = $_ -split '='
        $name = $parts[0]
        $value = $parts[1]

        # Set the environment variable
        [System.Environment]::SetEnvironmentVariable($name, $value, [System.EnvironmentVariableTarget]::Process)
    }
}

# Verify by listing the environment variables
Get-ChildItem Env: