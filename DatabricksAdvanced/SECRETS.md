## Databricks Secrets

Databricks Secrets allow you to securely store and access sensitive information, such as API keys, passwords, and tokens, within your Databricks workspace.

[Secrets Docs](https://docs.databricks.com/aws/en/security/secrets/?language=Databricks%C2%A0workspace%C2%A0UI)

<hr/>

### List secret scopes

To list the existing scopes in a workspace using the CLI:
```bash
databricks secrets list-scopes
```

Output:
```
{
  "scopes": [
    {
      "name": "my-scope",
      "backend_type": "DATABRICKS"
    }
  ]
}
```

<hr/>

### Create a secret

To create a secret scope using the CLI:
```bash
databricks secrets put-secret --json '{
  "scope": "<scope-name>",
  "key": "<key-name>",
  "string_value": "<secret>"
}'
```
<hr/>

### Read a secret

To read a secret from a scope using the CLI:
```bash
databricks secrets get-secret --scope <scope-name> --key <key-name> | jq -r .value | base64 --decode
```

```python
password = dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")
```

<hr/>

### List secrets

To list the secrets in a scope using the CLI:
```bash
databricks secrets list --scope <scope-name>
```
