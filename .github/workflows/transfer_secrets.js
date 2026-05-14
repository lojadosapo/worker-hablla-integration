name: Transfer Secrets
on: workflow_dispatch

jobs:
  copy-secrets:
    runs-on: ubuntu-latest
    steps:
      - name: Transfer
        env:
          GH_TOKEN: ${{ secrets.TRANSFER_TOKEN }}
          DEST_REPO: "lojadosapo/ls-data"
          HABLLA_BOARD_ID: ${{ secrets.HABLLA_BOARD_ID }}
          HABLLA_EMAIL: ${{ secrets.HABLLA_EMAIL }}
          HABLLA_PASSWORD: ${{ secrets.HABLLA_PASSWORD }}
          HABLLA_TOKEN: ${{ secrets.HABLLA_TOKEN }}
          HABLLA_WORKSPACE_ID: ${{ secrets.HABLLA_WORKSPACE_ID }}
        run: |
          gh secret set HABLLA_BOARD_ID --repo "$DEST_REPO" --body "$HABLLA_BOARD_ID"
          gh secret set HABLLA_EMAIL --repo "$DEST_REPO" --body "$HABLLA_EMAIL"
          gh secret set HABLLA_PASSWORD --repo "$DEST_REPO" --body "$HABLLA_PASSWORD"
          gh secret set HABLLA_TOKEN --repo "$DEST_REPO" --body "$HABLLA_TOKEN"
          gh secret set HABLLA_WORKSPACE_ID --repo "$DEST_REPO" --body "$HABLLA_WORKSPACE_ID"
