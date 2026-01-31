-- CreateExtension
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- CreateEnum
CREATE TYPE "UserFlag" AS ENUM ('red', 'white', 'green', 'yellow');

-- CreateEnum
CREATE TYPE "UserPermission" AS ENUM ('faqs', 'kick', 'users', 'chats', 'slots', 'blogs', 'merchs', 'rewards', 'casinos', 'providers', 'lootboxes', 'milestones', 'bonus_hunts', 'points_shop', 'promo_codes', 'tournaments', 'leaderboards', 'win_trackers', 'slots_battles', 'notifications', 'session_trackers');

-- CreateTable
CREATE TABLE "countries" (
    "country_code" TEXT NOT NULL,
    "currency_code" TEXT NOT NULL,
    "country_name_en" TEXT NOT NULL,
    "country_name_local" TEXT NOT NULL,
    "currency_name_en" TEXT NOT NULL,
    "tin_type" TEXT NOT NULL,
    "tin_name" TEXT NOT NULL,
    "official_language_code" TEXT NOT NULL,
    "official_language_name_en" TEXT NOT NULL,
    "official_language_name_local" TEXT NOT NULL,
    "country_calling_code" TEXT NOT NULL,
    "region" TEXT NOT NULL,
    "flag" TEXT NOT NULL,

    CONSTRAINT "countries_pkey" PRIMARY KEY ("country_code")
);

-- CreateTable
CREATE TABLE "users" (
    "id" UUID NOT NULL,
    "is_deleted" BOOLEAN NOT NULL DEFAULT false,
    "is_chat_blocked" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "chat_read_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "username" TEXT NOT NULL,
    "avatar_url" TEXT,
    "about" TEXT,
    "email" TEXT NOT NULL,
    "email_verified" BOOLEAN NOT NULL DEFAULT false,
    "policy_aggreed" BOOLEAN NOT NULL DEFAULT false,
    "multi_accounts_detected" BOOLEAN NOT NULL DEFAULT false,
    "password" TEXT,
    "telegram" TEXT,
    "kick_url" TEXT,
    "youtube_url" TEXT,
    "discord_id" TEXT,
    "discord_refresh_token" TEXT,
    "kick_id" INTEGER,
    "kick_slug" TEXT,
    "kick_access_token" TEXT,
    "kick_refresh_token" TEXT,
    "razed_id" TEXT,
    "razed_access_token" TEXT,
    "country_code" TEXT NOT NULL,
    "flag" "UserFlag" NOT NULL DEFAULT 'white',
    "permissions" "UserPermission"[] DEFAULT ARRAY[]::"UserPermission"[],
    "balance" INTEGER NOT NULL DEFAULT 0,
    "experience_points" INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT "users_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "countries:country_code:btree_asc" ON "countries"("country_code" ASC);

-- CreateIndex
CREATE INDEX "countries:country_code:btree_desc" ON "countries"("country_code" DESC);

-- CreateIndex
CREATE INDEX "countries:country_code:gin" ON "countries" USING GIN ("country_code" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "countries:currency_code:btree_asc" ON "countries"("currency_code" ASC);

-- CreateIndex
CREATE INDEX "countries:currency_code:btree_desc" ON "countries"("currency_code" DESC);

-- CreateIndex
CREATE INDEX "countries:currency_code:gin" ON "countries" USING GIN ("currency_code" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "countries:country_name_en:btree_asc" ON "countries"("country_name_en" ASC);

-- CreateIndex
CREATE INDEX "countries:country_name_en:btree_desc" ON "countries"("country_name_en" DESC);

-- CreateIndex
CREATE INDEX "countries:country_name_en:gin" ON "countries" USING GIN ("country_name_en" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "countries:currency_name_en:asc_btree" ON "countries"("currency_name_en" ASC);

-- CreateIndex
CREATE INDEX "countries:currency_name_en:desc_btree" ON "countries"("currency_name_en" DESC);

-- CreateIndex
CREATE INDEX "countries:currency_name_en:gin" ON "countries" USING GIN ("currency_name_en" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "countries:official_language_name_en:asc_btree" ON "countries"("official_language_name_en" ASC);

-- CreateIndex
CREATE INDEX "countries:official_language_name_en:desc_btree" ON "countries"("official_language_name_en" DESC);

-- CreateIndex
CREATE INDEX "countries:official_language_name_en:gin" ON "countries" USING GIN ("official_language_name_en" gin_trgm_ops);

-- CreateIndex
CREATE UNIQUE INDEX "users_username_key" ON "users"("username");

-- CreateIndex
CREATE UNIQUE INDEX "users_email_key" ON "users"("email");

-- CreateIndex
CREATE UNIQUE INDEX "users_discord_id_key" ON "users"("discord_id");

-- CreateIndex
CREATE UNIQUE INDEX "users_discord_refresh_token_key" ON "users"("discord_refresh_token");

-- CreateIndex
CREATE UNIQUE INDEX "users_kick_id_key" ON "users"("kick_id");

-- CreateIndex
CREATE UNIQUE INDEX "users_kick_slug_key" ON "users"("kick_slug");

-- CreateIndex
CREATE UNIQUE INDEX "users_kick_access_token_key" ON "users"("kick_access_token");

-- CreateIndex
CREATE UNIQUE INDEX "users_kick_refresh_token_key" ON "users"("kick_refresh_token");

-- CreateIndex
CREATE UNIQUE INDEX "users_razed_id_key" ON "users"("razed_id");

-- CreateIndex
CREATE UNIQUE INDEX "users_razed_access_token_key" ON "users"("razed_access_token");

-- CreateIndex
CREATE INDEX "users:created_at:asc_btree" ON "users"("created_at" ASC);

-- CreateIndex
CREATE INDEX "users:created_at:desc_btree" ON "users"("created_at" DESC);

-- CreateIndex
CREATE INDEX "users:updated_at:asc_btree" ON "users"("updated_at" ASC);

-- CreateIndex
CREATE INDEX "users:updated_at:desc_btree" ON "users"("updated_at" DESC);

-- CreateIndex
CREATE INDEX "users:is_deleted:asc_btree" ON "users"("is_deleted" ASC);

-- CreateIndex
CREATE INDEX "users:is_deleted:desc_btree" ON "users"("is_deleted" DESC);

-- CreateIndex
CREATE INDEX "users:discord_id:asc_btree" ON "users"("discord_id" ASC);

-- CreateIndex
CREATE INDEX "users:discord_id:desc_btree" ON "users"("discord_id" DESC);

-- CreateIndex
CREATE INDEX "users:kick_id:asc_btree" ON "users"("kick_id" ASC);

-- CreateIndex
CREATE INDEX "users:kick_id:desc_btree" ON "users"("kick_id" DESC);

-- CreateIndex
CREATE INDEX "users:razed_id:asc_btree" ON "users"("razed_id" ASC);

-- CreateIndex
CREATE INDEX "users:razed_id:desc_btree" ON "users"("razed_id" DESC);

-- CreateIndex
CREATE INDEX "users:username:asc_btree" ON "users"("username" ASC);

-- CreateIndex
CREATE INDEX "users:username:desc_btree" ON "users"("username" DESC);

-- CreateIndex
CREATE INDEX "users:username:gin" ON "users" USING GIN ("username" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "users:email:asc_btree" ON "users"("email" ASC);

-- CreateIndex
CREATE INDEX "users:email:desc_btree" ON "users"("email" DESC);

-- CreateIndex
CREATE INDEX "users:email:gin" ON "users" USING GIN ("email" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "users:telegram:asc_btree" ON "users"("telegram" ASC);

-- CreateIndex
CREATE INDEX "users:telegram:desc_btree" ON "users"("telegram" DESC);

-- CreateIndex
CREATE INDEX "users:telegram:gin" ON "users" USING GIN ("telegram" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "users:permissions:gin" ON "users" USING GIN ("permissions" array_ops);

-- CreateIndex
CREATE INDEX "users:flag:asc_btree" ON "users"("flag" ASC);

-- CreateIndex
CREATE INDEX "users:flag:desc_btree" ON "users"("flag" DESC);

-- CreateIndex
CREATE INDEX "users:balance:asc_btree" ON "users"("balance" ASC);

-- CreateIndex
CREATE INDEX "users:balance:desc_btree" ON "users"("balance" DESC);

-- CreateIndex
CREATE INDEX "users:username_password:btree" ON "users"("username", "password");

-- CreateIndex
CREATE INDEX "users:email_password:btree" ON "users"("email", "password");

-- AddForeignKey
ALTER TABLE "users" ADD CONSTRAINT "users_country_code_fkey" FOREIGN KEY ("country_code") REFERENCES "countries"("country_code") ON DELETE CASCADE ON UPDATE CASCADE;
