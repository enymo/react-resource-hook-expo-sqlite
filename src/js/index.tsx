import { CacheResourceBackendAdapter, Resource } from "@enymo/react-resource-hook";
import { SQLiteDatabase, SQLiteProvider, useSQLiteContext } from "expo-sqlite";
import { ReactNode } from "react";

interface DatabaseResource {
    resource: string,
    key: string,
    target: "local" | "remote",
    id: string,
    data: string
}

const DATABASE_VERSION = 1;

const methodNotSupported = () => {
    throw new Error("Method not supported");
}

async function migrateDb(db: SQLiteDatabase) {
    const { user_version: currentDbVersion } = (await db.getFirstAsync<{ user_version: number }>("PRAGMA user_version"))!;
    if (currentDbVersion >= DATABASE_VERSION) return;

    switch (currentDbVersion) {
        case 0:
            await db.execAsync(`
                CREATE TABLE resources (
                    resource TEXT NOT NULL,
                    key TEXT NULL,
                    target TEXT NOT NULL,
                    id TEXT NOT NULL,
                    data TEXT NULL,
                    PRIMARY KEY (resource, key, target, id)
                )
            `);
            break;
    }

    await db.execAsync(`PRAGMA user_version = ${DATABASE_VERSION}`);
}

export function ExpoSQLiteResourceProvider({databaseName, children}: {
    databaseName: string,
    children: ReactNode
}) {
    return (
        <SQLiteProvider databaseName={databaseName} onInit={migrateDb}>
            {children}
        </SQLiteProvider>
    )
}

export default function createExpoSQLiteResourceAdapter({}: {}): CacheResourceBackendAdapter<{}, {}, never> {
    return (resource, {}, cache) => ({
        actionHook: ({}, params) => {
            const db = useSQLiteContext();
            const key = JSON.stringify(params) ?? "";

            return {
                store: async data => {
                    const promises = [
                        db.runAsync(
                            "INSERT INTO resources (resource, key, target, id, data) VALUES (?, ?, 'local', ?, ?)",
                            resource,
                            key,
                            JSON.stringify(data.id),
                            JSON.stringify(data)
                        )
                    ];
                    if (cache) {
                        promises.push(db.runAsync(
                            "INSERT INTO resources (resource, key, target, id, data) VALUES (?, ?, 'remote', ?, null)",
                            resource,
                            key,
                            JSON.stringify(data.id)
                        ));
                    }
                    await Promise.all(promises);
                    return data;
                },
                batchStore: async data => {
                    await Promise.all(data.flatMap(item => {
                        const promises = [
                            db.runAsync(
                                "INSERT INTO resources (resource, key, target, id, data) VALUES (?, ?, 'local', ?, ?)",
                                resource,
                                key,
                                JSON.stringify(item.id),
                                JSON.stringify(item)
                            )
                        ];
                        if (cache) {
                            promises.push(db.runAsync(
                                "INSERT INTO resources (resource, key, target, id, data) VALUES (?, ?, 'remote', ?, null)",
                                resource,
                                key,
                                JSON.stringify(item.id)
                            ));
                        }
                        return promises;
                    }));
                    return data;
                },
                update: async (id, data) => {
                    if (cache && (await db.getFirstAsync(
                        "SELECT * FROM resources WHERE resource = ? AND key = ? AND target = 'remote' AND id = ?",
                        resource,
                        key,
                        JSON.stringify(id)
                    )) === null) {
                        await db.runAsync(
                            "INSERT INTO resources (resource, key, target, id, data) SELECT resource, key, 'remote', id, data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                            resource,
                            key,
                            JSON.stringify(id)
                        );
                    }
                    await db.runAsync(
                        "UPDATE resources SET data = json_patch(data, ?) WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                        JSON.stringify(data),
                        resource,
                        key,
                        JSON.stringify(id)
                    );
                    return JSON.parse((await db.getFirstAsync<DatabaseResource>(
                        "SELECT data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                        resource,
                        key,
                        JSON.stringify(id)
                    ))!.data);
                },
                batchUpdate: async data => {
                    return Promise.all(data.map(async item => {
                        const {id, ...rest} = item;
                        if (cache && (await db.getFirstAsync(
                            "SELECT * FROM resources WHERE resource = ? AND key = ? AND target = 'remote' AND id = ?",
                            resource,
                            key,
                            JSON.stringify(id)
                        )) === null) {
                            await db.runAsync(
                                "INSERT INTO resources (resource, key, target, id, data) SELECT resource, key, 'remote', id, data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                                resource,
                                key,
                                JSON.stringify(id)
                            );
                        }
                        await db.runAsync(
                            "UPDATE resources SET data = json_patch(data, ?) WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                            JSON.stringify(rest),
                            resource,
                            key,
                            JSON.stringify(id)
                        );
                        return JSON.parse((await db.getFirstAsync<DatabaseResource>(
                            "SELECT data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                            resource,
                            key,
                            JSON.stringify(id)
                        ))!.data);
                    }))
                },
                destroy: async id => {
                    if (cache && (await db.getFirstAsync(
                        "SELECT * FROM resources WHERE resource = ? AND key = ? AND target = 'remote' AND id = ?",
                        resource,
                        key,
                        JSON.stringify(id)
                    )) === null) {
                        await db.runAsync(
                            "INSERT INTO resources (resource, key, target, id, data) SELECT resource, key, 'remote', id, data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                            resource,
                            key,
                            JSON.stringify(id)
                        );
                    }
                    await db.runAsync(
                        "DELETE FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                        resource,
                        key,
                        JSON.stringify(id)
                    );
                },
                batchDestroy: async ids => {
                    await Promise.all(ids.map(async id => {
                        if (cache && (await db.getFirstAsync(
                            "SELECT * FROM resources WHERE resource = ? AND key = ? AND target = 'remote' AND id = ?",
                            resource,
                            key,
                            id.toString()
                        )) === null) {
                            await db.runAsync(
                                "INSERT INTO resources (resource, key, target, id, data) SELECT resource, key, 'remote', id, data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                                resource,
                                key,
                                JSON.stringify(id)
                            );
                        }
                        await db.runAsync(
                            "DELETE FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                            resource,
                            key,
                            JSON.stringify(id)
                        );
                    }))
                },
                query: methodNotSupported,
                refresh: async id => {
                    if (id !== undefined) {
                        const data = await db.getFirstAsync<DatabaseResource>(
                            "SELECT data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                            resource,
                            key,
                            JSON.stringify(id)
                        );
                        return {
                            data: data ? JSON.parse(data.resource) : null,
                            meta: undefined as any,
                            error: null
                        }
                    }
                    else {
                        const data = await db.getAllAsync<DatabaseResource>(
                            "SELECT data FROM resources WHERE resource = ? AND key = ? AND target = 'local'",
                            resource,
                            key
                        );
                        return {
                            data: data.map(item => JSON.parse(item.data)),
                            meta: undefined as any,
                            error: null
                        };
                    }
                },
                getCache: async () => {
                    const map = new Map<Resource["id"], {
                        id: Resource["id"],
                        local: Resource | null,
                        remote?: Resource | null
                    }>;

                    const data = await db.getAllAsync<DatabaseResource>(
                        "SELECT * FROM resources WHERE resource = ? AND key = ?",
                        resource,
                        key
                    );

                    for (const entry of data) {
                        const id = JSON.parse(entry.id);
                        if (!map.has(id)) {
                            map.set(id, {
                                id,
                                local: null,
                                [entry.target]: JSON.parse(entry.data)
                            });
                        }
                        else {
                            map.get(id)![entry.target] = JSON.parse(entry.data);
                        }
                    }

                    return [...map.values()] as any;
                },
                sync: async (...ids) => {
                    await db.runAsync(
                        `DELETE FROM resources WHERE resource = ? AND key = ? AND target = 'remote' AND id IN (${Array(ids.length).fill("?").join(",")})`,
                        resource,
                        key,
                        ...ids.map(id => JSON.stringify(id))
                    )
                },
                addOfflineListener: () => () => undefined
            }
        },
        eventHook: () => {}
    })
}