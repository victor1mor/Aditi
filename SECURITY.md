# Segurança e escopo

Aditi é um motor de download de cloud drives anterior à 1.0, distribuído como código-fonte por este repositório.
Não passou por auditoria de segurança independente. Verificações automáticas não garantem ausência de falhas.
Veja o CHANGELOG.md para o que cada versão mudou e seus limites.

## Fronteira de confiança

- Todo servidor remoto é tratado como não confiável: o tamanho e, quando a origem declara, o checksum de cada
  arquivo são conferidos antes de o arquivo virar destino; o `.aditi.part` divergente é descartado.
- Credenciais vêm de flags ou de variáveis de ambiente (`ADITI_*_TOKEN`). O Aditi só grava token em disco quando
  você pede (`--token-out` ou `--refresh-token-out`); em Linux e macOS o arquivo nasce legível só pelo dono, no
  Windows segue a permissão da pasta. Guarde esses arquivos em pasta que só você controla.
- O `--proxies` usa os proxies que você indicar. Proxy aberto de terceiro vê para onde você se conecta.
- Administradores locais e processos rodando com o seu usuário estão dentro da fronteira de confiança.

## Como relatar uma falha

Relate em privado ao dono do repositório pelo GitHub. Não abra issue pública com detalhes de exploração. Não há
recompensa nem prazo de resposta prometido.
