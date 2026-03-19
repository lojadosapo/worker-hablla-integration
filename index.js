const axios = require('axios');

async function run() {
    const googleToken = process.env.GOOGLE_TOKEN;
    const habllaEmail = process.env.HABLLA_EMAIL;
    const habllaPassword = process.env.HABLLA_PASSWORD;
    const workspaceId = process.env.HABLLA_WORKSPACE_ID;
    const spreadsheetId = process.env.SPREADSHEET_ID;
    const boardId = process.env.HABLLA_BOARD_ID;

    try {
        console.log(`[${new Date().toISOString()}] Iniciando Processamento Hablla...`);

        // 1. Autenticação Hablla
        const loginRes = await axios.post('https://api.hablla.com/v1/authentication/login', {
            email: habllaEmail,
            password: habllaPassword
        });
        const habllaToken = loginRes.data.accessToken;
        const habllaHeaders = { 'Authorization': `Bearer ${habllaToken}` };
        const googleHeaders = { 
            'Authorization': `Bearer ${googleToken}`,
            'Content-Type': 'application/json'
        };

        // --- FLUXO 1: CARDS (COM PAGINAÇÃO) ---
        console.log(`[${new Date().toISOString()}] Processando Cards...`);
        let currentPage = 1;
        let totalPages = 1;

        while (currentPage <= totalPages) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${workspaceId}/cards`, {
                params: { board: boardId, limit: 50, order: 'updated_at', page: currentPage },
                headers: habllaHeaders
            });

            totalPages = res.data.totalPages;
            const cards = res.data.results || [];
            const rowsCards = cards.map(card => {
                const formatDate = (d) => d ? new Date(new Date(d).getTime() - (3 * 3600000)).toLocaleString('pt-BR') : "";
                
                // Mapeamento de Custom Fields (IDs Fixos conforme seu código original)
                let cf1 = "", cf2 = "", cf3 = "", cf4 = "";
                (card.custom_fields || []).forEach(cf => {
                    if (cf.custom_field === "67b39131ee792966f3fba492") cf1 = cf.value;
                    else if (cf.custom_field === "67b608470787782ce7acafba") cf2 = cf.value;
                    else if (cf.custom_field === "67dc6a0a17925c23d8365708") cf3 = cf.value;
                    else if (cf.custom_field === "679120ec177ff6d2c7597156") cf4 = cf.value;
                });

                const tags = (card.tags || []).map(t => t.name).join(", ");

                return [
                    formatDate(card.updated_at), formatDate(card.created_at), card.workspace, card.board,
                    card.list, cf1, cf2, cf3, card.name, card.description, card.source, card.status,
                    card.user, formatDate(card.finished_at), card.id, "", cf4, tags
                ];
            });

            if (rowsCards.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/Base%20Hablla%20Card%20-%20Pendente!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsCards }, { headers: googleHeaders });
            }
            currentPage++;
        }

        // --- FLUXO 2: ATENDENTES (ONTEM) ---
        console.log(`[${new Date().toISOString()}] Processando Atendentes...`);
        const ontem = new Date();
        ontem.setDate(ontem.getDate() - 1);
        const dateStr = ontem.toISOString().split('T')[0];

        const resAtendentes = await axios.get(`https://api.hablla.com/v1/workspaces/${workspaceId}/reports/services/summary`, {
            params: { start_date: `${dateStr}T00:00:00Z`, end_date: `${dateStr}T23:59:59Z` },
            headers: habllaHeaders
        });

        const rowsAtendentes = (resAtendentes.data.results || []).map(item => {
            const user = item.user || {};
            const sector = item.sector || {};
            const conn = item.connection || {};
            return [
                ontem.toLocaleDateString('pt-BR'), workspaceId, sector.id, sector.name,
                user.id, "", user.email, item.total_services, item.tme, item.tma,
                conn.id, conn.name, conn.type, item.total_csat, item.total_csat_greater_4,
                item.csat, item.total_fcr
            ];
        });

        if (rowsAtendentes.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, 
            { values: rowsAtendentes }, { headers: googleHeaders });
        }

        console.log(`[${new Date().toISOString()}] Sucesso Total.`);
    } catch (err) {
        console.error(`[${new Date().toISOString()}] Erro na execução.`);
        process.exit(1);
    }
}

run();
