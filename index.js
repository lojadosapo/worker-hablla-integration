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

        // 1. Sincroniza nomes dos colaboradores (Busca na planilha de DB)
        console.log(`[${new Date().toISOString()}] Sincronizando base de colaboradores...`);
        const resDB = await axios.get(
            `https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`,
            { headers: gHeaders }
        );
        
        const mapaNomes = {};
        if (resDB.data && resDB.data.values) {
            resDB.data.values.forEach(row => {
                if (row[12]) mapaNomes[row[12]] = row[0]; // Mapeia ID Hablla -> Nome
            });
            console.log(`[${new Date().toISOString()}] ${Object.keys(mapaNomes).length} colaboradores mapeados.`);
        }

        // 2. Login na API da Hablla
        console.log(`[${new Date().toISOString()}] Autenticando na Hablla...`);
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', {
            email: HABLLA_EMAIL, password: HABLLA_PASSWORD
        });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- LÓGICA DE DATAS (CARGA INICIAL HOJE VS DIÁRIO AMANHÃ) ---
        const hojeData = new Date().toISOString().split('T')[0]; // Ex: 2026-03-19
        let inicioBusca = new Date();
        
        // Se for dia 19/03/2026, busca desde o início do ano até 18/03/2026
        if (hojeData === "2026-03-19") {
            console.log("--- EXECUTANDO CARGA CONSOLIDADA (DE 01/01/2026 ATÉ ONTEM) ---");
            inicioBusca = new Date("2026-01-01T00:00:00Z");
        } else {
            // A partir de amanhã, pega apenas o dia anterior (00:00:00)
            inicioBusca.setDate(inicioBusca.getDate() - 1);
            inicioBusca.setHours(0, 0, 0, 0);
        }

        // O limite final é sempre ontem às 23:59:59 (para não pegar dados incompletos de hoje)
        const fimBusca = new Date();
        fimBusca.setDate(fimBusca.getDate() - 1);
        fimBusca.setHours(23, 59, 59, 999);

        const dISOInicio = inicioBusca.toISOString();
        const dISOFim = fimBusca.toISOString();
        const dRelatorio = inicioBusca.toLocaleDateString('pt-BR');

        console.log(`[${new Date().toISOString()}] Intervalo: ${dISOInicio} até ${dISOFim}`);

        // 3. Processa Cards (Envia para aba 'Base Hablla Card')
        console.log(`[${new Date().toISOString()}] Buscando cards na Hablla...`);
        let page = 1, totalPages = 1;
        while (page <= totalPages) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { 
                    board: HABLLA_BOARD_ID, 
                    limit: 50, 
                    order: 'updated_at', 
                    page: page,
                    updated_after: dISOInicio,
                    updated_before: dISOFim
                },
                headers: hHeaders
            });

            totalPages = res.data.totalPages || 1;
            console.log(`[${new Date().toISOString()}] Processando página ${page} de ${totalPages}...`);

            const rowsCards = (res.data.results || []).map(card => {
                const fmt = (d) => d ? new Date(new Date(d).getTime() - (3 * 3600000)).toLocaleString('pt-BR') : "";
                
                let cf = ["", "", "", ""];
                const ids = ["67b39131ee792966f3fba492", "67b608470787782ce7acafba", "67dc6a0a17925c23d8365708", "679120ec177ff6d2c7597156"];
                (card.custom_fields || []).forEach(f => {
                    const idx = ids.indexOf(f.custom_field);
                    if (idx !== -1) cf[idx] = f.value;
                });

                return [
                    fmt(card.updated_at), fmt(card.created_at), card.workspace, card.board, card.list,
                    cf[0], cf[1], cf[2], card.name, card.description, card.source, card.status,
                    card.user, fmt(card.finished_at), card.id, 
                    mapaNomes[card.user] || "", 
                    cf[3], (card.tags || []).map(t => t.name).join(", ")
                ];
            });

            if (rowsCards.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsCards }, { headers: gHeaders });
            }
            page++;
        }

        // 4. Processa Atendentes (Relatório Consolidado de Ontem)
        console.log(`[${new Date().toISOString()}] Gerando relatório de atendentes...`);
        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dISOInicio, end_date: dISOFim },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [
                dRelatorio, HABLLA_WORKSPACE_ID, s.id, s.name, u.id, 
                mapaNomes[u.id] || "", u.email, item.total_services, 
                item.tme, item.tma, c.id, c.name, c.type,
                item.total_csat, item.total_csat_greater_4, item.csat, item.total_fcr
            ];
        });

        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, 
            { values: rowsAt }, { headers: gHeaders });
        }
        console.log(`[${new Date().toISOString()}] Processamento concluído com sucesso.`);

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
