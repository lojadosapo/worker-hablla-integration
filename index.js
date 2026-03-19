const axios = require('axios');

async function run() {
    const { 
        GOOGLE_TOKEN, 
        HABLLA_EMAIL, 
        HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, 
        HABLLA_BOARD_ID, 
        SPREADSHEET_ID,
        DB_COLABORADOR_ID 
    } = process.env;

    try {
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

        // 1. Busca nomes dos colaboradores
        console.log(`[${new Date().toISOString()}] Sincronizando base de colaboradores...`);
        const resDB = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`,
            { headers: gHeaders }
        );
        
        const mapaNomes = {};
        if (resDB.data && resDB.data.values) {
            resDB.data.values.forEach(row => {
                const nome = row[0];   // Coluna A
                const idHablla = row[12]; // Coluna M
                if (idHablla) mapaNomes[idHablla] = nome;
            });
            console.log(`[${new Date().toISOString()}] ${Object.keys(mapaNomes).length} colaboradores mapeados.`);
        } else {
            console.log(`[${new Date().toISOString()}] Aviso: Nenhum dado encontrado na planilha de colaboradores.`);
        }

        // 2. Login Hablla
        console.log(`[${new Date().toISOString()}] Autenticando na Hablla...`);
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', {
            email: HABLLA_EMAIL, password: HABLLA_PASSWORD
        });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // 3. Processa Cards (Fluxo 1)
        console.log(`[${new Date().toISOString()}] Buscando Cards...`);
        let page = 1, totalPages = 1;
        while (page <= totalPages) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, limit: 50, order: 'updated_at', page: page },
                headers: hHeaders
            });

            totalPages = res.data.totalPages;
            const rowsCards = (res.data.results || []).map(card => {
                const fmt = (d) => d ? new Date(new Date(d).getTime() - (3 * 3600000)).toLocaleString('pt-BR') : "";
                
                let cf1 = "", cf2 = "", cf3 = "", cf4 = "";
                (card.custom_fields || []).forEach(cf => {
                    if (cf.custom_field === "67b39131ee792966f3fba492") cf1 = cf.value;
                    else if (cf.custom_field === "67b608470787782ce7acafba") cf2 = cf.value;
                    else if (cf.custom_field === "67dc6a0a17925c23d8365708") cf3 = cf.value;
                    else if (cf.custom_field === "679120ec177ff6d2c7597156") cf4 = cf.value;
                });

                return [
                    fmt(card.updated_at), fmt(card.created_at), card.workspace, card.board, card.list,
                    cf1, cf2, cf3, card.name, card.description, card.source, card.status,
                    card.user, fmt(card.finished_at), card.id, 
                    mapaNomes[card.user] || "", 
                    cf4, (card.tags || []).map(t => t.name).join(", ")
                ];
            });

            if (rowsCards.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card%20-%20Pendente!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsCards }, { headers: gHeaders });
            }
            page++;
        }

        // 4. Processa Atendentes (Fluxo 2)
        console.log(`[${new Date().toISOString()}] Gerando relatório de atendentes...`);
        const ontem = new Date(); ontem.setDate(ontem.getDate() - 1);
        const dRel = ontem.toLocaleDateString('pt-BR');
        const dISO = ontem.toISOString().split('T')[0];

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: `${dISO}T00:00:00Z`, end_date: `${dISO}T23:59:59Z` },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [
                dRel, HABLLA_WORKSPACE_ID, s.id, s.name, u.id, 
                mapaNomes[u.id] || "",
                u.email, item.total_services, item.tme, item.tma, c.id, c.name, c.type,
                item.total_csat, item.total_csat_greater_4, item.csat, item.total_fcr
            ];
        });

        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, 
            { values: rowsAt }, { headers: gHeaders });
        }
        console.log(`[${new Date().toISOString()}] Processamento concluído.`);

    } catch (e) {
        console.error("--- ERRO DETALHADO PARA QA ---");
        if (e.response) {
            console.error("Status da API:", e.response.status);
            console.error("Detalhes:", JSON.stringify(e.response.data, null, 2));
        } else {
            console.error("Mensagem de erro:", e.message);
        }
        process.exit(1);
    }
}
run();
