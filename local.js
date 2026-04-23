require('dotenv').config();
const axios = require('axios');

async function runLocalHablla() {
    const { HABLLA_EMAIL, HABLLA_PASSWORD, HABLLA_WORKSPACE_ID, HABLLA_TOKEN } = process.env;

    if (!HABLLA_WORKSPACE_ID) {
        console.error("ERRO: HABLLA_WORKSPACE_ID não definido no .env");
        return;
    }

    let token = HABLLA_TOKEN;
    let isWorkspaceToken = false;

    if (!token) {
        if (!HABLLA_EMAIL || !HABLLA_PASSWORD) {
            console.error("ERRO: Credenciais HABLLA_EMAIL e HABLLA_PASSWORD necessárias no .env para login");
            return;
        }

        try {
            console.log(">>> Fazendo login no Hablla...");
            const loginResponse = await axios.post('https://api.hablla.com/v1/authentication/login', {
                email: HABLLA_EMAIL,
                password: HABLLA_PASSWORD
            });
            token = loginResponse.data.accessToken;
            console.log("Login realizado com sucesso. Token obtido.");
        } catch (error) {
            console.error("ERRO no login:", error.response?.data || error.message);
            console.log("\nComo o login da API falhou, você precisa obter o token manualmente:");
            console.log("1. Abra https://app.hablla.com no navegador");
            console.log("2. Faça login com seu email e senha");
            console.log("3. Abra as ferramentas de desenvolvedor (F12)");
            console.log("4. Vá para a aba 'Network' (Rede)");
            console.log("5. Faça alguma ação no Hablla (ex: abrir um card ou lista)");
            console.log("6. Procure por uma requisição para 'api.hablla.com' na lista");
            console.log("7. Clique na requisição e vá para 'Headers'");
            console.log("8. Copie o valor do header 'authorization' (ex: 'Bearer eyJhbGci...' ou apenas o token)");
            console.log("9. Cole o token no .env como HABLLA_TOKEN");
            console.log("\nPara Workspace Token (recomendado, não expira):");
            console.log("- Execute um fluxo no Hablla Studio que use componente API");
            console.log("- O token estará no header Authorization da resposta");
            return;
        }
    } else {
        // Detectar tipo de token
        if (token.startsWith('ey')) {
            console.log(">>> Usando User Token (expira em ~1 hora)");
        } else {
            console.log(">>> Usando Workspace Token (não expira)");
            isWorkspaceToken = true;
        }
    }

    const headers = {
        'Authorization': isWorkspaceToken ? token : `Bearer ${token}`,
        'accept': 'application/json'
    };

    try {
        console.log(">>> Buscando persons...");
        const response = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/persons?page=1&limit=50`, {
            headers,
            timeout: 10000
        });

        console.log("Dados recebidos:");
        console.log("Status:", response.status);
        console.log("Items:", response.data.results?.length || response.data.length || "unknown");
        console.log("Primeiro item (se existir):", response.data.results?.[0] || response.data[0] || "N/A");
    } catch (error) {
        console.error("ERRO na requisição:", error.response?.data || error.message);
    }
}

runLocalHablla();