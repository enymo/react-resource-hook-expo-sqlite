import { createRequiredContext } from "@enymo/react-better-context";
import { CacheResourceBackendAdapter, Resource } from "@enymo/react-resource-hook";
import { dateTransformer, identity, inverseDateTransformer } from "@enymo/react-resource-hook-util";
import { openDatabaseAsync, SQLiteDatabase } from "expo-sqlite";
import { ReactNode, useEffect, useRef, useState } from "react";

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

const [Provider, useContext] = createRequiredContext<SQLiteDatabase>("ExpoSQLiteResourceProvider must be present in component tree");

export function ExpoSQLiteResourceProvider({databaseName, fallback = null, children}: {
    databaseName: string,
    fallback?: ReactNode,
    children: ReactNode
}) {
    const [db, setDb] = useState<SQLiteDatabase>();
    const dbRef = useRef<SQLiteDatabase>(undefined);

    useEffect(() => {
        const abortController = new AbortController;
        (async () => {
            const db = await openDatabaseAsync(databaseName);
            await migrateDb(db);
            if (abortController.signal.aborted) {
                db.closeAsync()
            }
            else {
                dbRef.current = db;
                setDb(db);
            }
        })();
        return () => {
            abortController.abort();
            dbRef.current?.closeAsync();
        }
    }, [setDb, dbRef]);

    if (db === undefined) return fallback;

    return (
        <Provider value={db}>
            {children}
        </Provider>
    )
}

export default function createExpoSQLiteResourceAdapter({}: {}): CacheResourceBackendAdapter<{
    transformer?: (item: any) => Promise<any> | any,
    inverseTransformer?: (item: any) => Promise<any> | any,
    transformDates?: boolean
}, {}, never> {
    return (resource, {
        transformer: baseTransformer = identity,
        inverseTransformer: baseInverseTransformer = identity,
        transformDates = false
    }, cache) => {
        const transformer = transformDates ? async (item: any) => dateTransformer(await baseTransformer(item)) : baseTransformer;
        const inverseTransformer = transformDates ? async (item: any) => inverseDateTransformer(await baseInverseTransformer(item)) : inverseDateTransformer;

        return {
            actionHook: ({}, params) => {
                const db = useContext();
                const key = JSON.stringify(params) ?? "";

                return {
                    store: async data => {
                        const promises = [
                            db.runAsync(
                                "INSERT INTO resources (resource, key, target, id, data) VALUES (?, ?, 'local', ?, ?)",
                                resource,
                                key,
                                JSON.stringify(data.id),
                                JSON.stringify(await inverseTransformer(data))
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
                                (async () => db.runAsync(
                                    "INSERT INTO resources (resource, key, target, id, data) VALUES (?, ?, 'local', ?, ?)",
                                    resource,
                                    key,
                                    JSON.stringify(item.id),
                                    JSON.stringify(await inverseTransformer(item))
                                ))()
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
                            JSON.stringify(await inverseTransformer(data)),
                            resource,
                            key,
                            JSON.stringify(id)
                        );
                        return transformer(JSON.parse((await db.getFirstAsync<DatabaseResource>(
                            "SELECT data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                            resource,
                            key,
                            JSON.stringify(id)
                        ))!.data));
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
                                JSON.stringify(await inverseTransformer(rest)),
                                resource,
                                key,
                                JSON.stringify(id)
                            );
                            return transformer(JSON.parse((await db.getFirstAsync<DatabaseResource>(
                                "SELECT data FROM resources WHERE resource = ? AND key = ? AND target = 'local' AND id = ?",
                                resource,
                                key,
                                JSON.stringify(id)
                            ))!.data));
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
                                data: data ? await transformer(JSON.parse(data.resource)) : null,
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
                                data: await Promise.all(data.map(item => transformer(JSON.parse(item.data)))),
                                meta: undefined as any,
                                error: null
                            };
                        }
                    },
                    getCache: async () => {
                        const data = await Promise.all((await db.getAllAsync<DatabaseResource>(
                            "SELECT * FROM resources WHERE resource = ? AND key = ?",
                            resource,
                            key
                        )).map(async ({data, ...entry}) => ({
                            ...entry,
                            data: await transformer(JSON.parse(data)) 
                        })));

                        const map = new Map<Resource["id"], {
                            id: Resource["id"],
                            local: Resource | null,
                            remote?: Resource | null
                        }>;

                        for (const entry of data) {
                            const id = JSON.parse(entry.id);
                            if (!map.has(id)) {
                                map.set(id, {
                                    id,
                                    local: null,
                                    [entry.target]: entry.data
                                });
                            }
                            else {
                                map.get(id)![entry.target] = entry.data;
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
        }
    }
}