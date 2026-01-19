import {
  pgTable,
  serial,
  text,
  integer,
  boolean,
  timestamp,
  decimal,
  json,
  primaryKey,
  foreignKey,
  index,
  pgEnum,
  type PgTableWithColumns,
} from 'drizzle-orm/pg-core'
import { sql } from 'drizzle-orm/sql'
import {
  sqliteTable,
  integer as sqliteInteger,
  text as sqliteText,
  real as sqliteReal,
  blob as sqliteBlob,
  type SQLiteTableWithColumns,
} from 'drizzle-orm/sqlite-core'

// PostgreSQL Enums
export const planEnum = pgEnum('Plan', ['FREE', 'STARTER', 'PRO', 'ENTERPRISE'])
export const memberRoleEnum = pgEnum('MemberRole', [
  'OWNER',
  'ADMIN',
  'MEMBER',
  'VIEWER',
])
export const projectStatusEnum = pgEnum('ProjectStatus', [
  'ACTIVE',
  'ARCHIVED',
  'COMPLETED',
  'ON_HOLD',
])
export const taskStatusEnum = pgEnum('TaskStatus', [
  'TODO',
  'IN_PROGRESS',
  'IN_REVIEW',
  'DONE',
  'CANCELLED',
])
export const priorityEnum = pgEnum('Priority', [
  'LOW',
  'MEDIUM',
  'HIGH',
  'URGENT',
])

// PostgreSQL Schema
export const pgOrganizations = pgTable('organizations', {
  id: serial('id').primaryKey(),
  name: text('name').notNull(),
  slug: text('slug').notNull().unique(),
  plan: planEnum('plan').notNull().default('FREE'),
  settings: json('settings'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
})

export const pgUsers = pgTable('users', {
  id: serial('id').primaryKey(),
  email: text('email').notNull().unique(),
  name: text('name'),
  avatarUrl: text('avatarUrl'),
  role: text('role').notNull().default('USER'),
  status: text('status').notNull().default('ACTIVE'),
  metadata: text('metadata'),
  tags: text('tags').notNull().default('[]'),
  lastLoginAt: timestamp('lastLoginAt'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
})

export const pgMembers = pgTable('members', {
  id: serial('id').primaryKey(),
  role: memberRoleEnum('role').notNull().default('MEMBER'),
  joinedAt: timestamp('joinedAt').notNull().defaultNow(),
  organizationId: integer('organizationId')
    .notNull()
    .references(() => pgOrganizations.id, { onDelete: 'cascade' }),
  userId: integer('userId')
    .notNull()
    .references(() => pgUsers.id, { onDelete: 'cascade' }),
})

export const pgProjects = pgTable('projects', {
  id: serial('id').primaryKey(),
  name: text('name').notNull(),
  description: text('description'),
  color: text('color'),
  status: projectStatusEnum('status').notNull().default('ACTIVE'),
  isPublic: boolean('isPublic').notNull().default(false),
  budget: decimal('budget', { precision: 10, scale: 2 }),
  startDate: timestamp('startDate'),
  endDate: timestamp('endDate'),
  metadata: json('metadata'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
  organizationId: integer('organizationId')
    .notNull()
    .references(() => pgOrganizations.id, { onDelete: 'cascade' }),
})

export const pgMilestones = pgTable('milestones', {
  id: serial('id').primaryKey(),
  name: text('name').notNull(),
  description: text('description'),
  dueDate: timestamp('dueDate'),
  completedAt: timestamp('completedAt'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  projectId: integer('projectId')
    .notNull()
    .references(() => pgProjects.id, { onDelete: 'cascade' }),
})

export const pgLabels = pgTable('labels', {
  id: serial('id').primaryKey(),
  name: text('name').notNull(),
  color: text('color').notNull(),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  projectId: integer('projectId')
    .notNull()
    .references(() => pgProjects.id, { onDelete: 'cascade' }),
})

export const pgTasks: PgTableWithColumns<any> = pgTable('tasks', {
  id: serial('id').primaryKey(),
  title: text('title').notNull(),
  description: text('description'),
  status: taskStatusEnum('status').notNull().default('TODO'),
  priority: priorityEnum('priority').notNull().default('MEDIUM'),
  position: integer('position').notNull().default(0),
  dueDate: timestamp('dueDate'),
  completedAt: timestamp('completedAt'),
  estimatedHours: decimal('estimatedHours', { precision: 10, scale: 2 }),
  metadata: json('metadata'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
  projectId: integer('projectId')
    .notNull()
    .references(() => pgProjects.id, { onDelete: 'cascade' }),
  assigneeId: integer('assigneeId').references(() => pgUsers.id, {
    onDelete: 'set null',
  }),
  creatorId: integer('creatorId')
    .notNull()
    .references(() => pgUsers.id, { onDelete: 'cascade' }),
  milestoneId: integer('milestoneId').references(() => pgMilestones.id, {
    onDelete: 'set null',
  }),
  parentId: integer('parentId').references((): any => pgTasks.id, {
    onDelete: 'set null',
  }),
})

export const pgTaskLabels = pgTable(
  'task_labels',
  {
    taskId: integer('taskId')
      .notNull()
      .references(() => pgTasks.id, { onDelete: 'cascade' }),
    labelId: integer('labelId')
      .notNull()
      .references(() => pgLabels.id, { onDelete: 'cascade' }),
    assignedAt: timestamp('assignedAt').notNull().defaultNow(),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.taskId, table.labelId] }),
  }),
)

export const pgComments: PgTableWithColumns<any> = pgTable('comments', {
  id: serial('id').primaryKey(),
  content: text('content').notNull(),
  isEdited: boolean('isEdited').notNull().default(false),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
  taskId: integer('taskId')
    .notNull()
    .references(() => pgTasks.id, { onDelete: 'cascade' }),
  authorId: integer('authorId')
    .notNull()
    .references(() => pgUsers.id, { onDelete: 'cascade' }),
  parentId: integer('parentId').references((): any => pgComments.id, {
    onDelete: 'cascade',
  }),
})

export const pgReactions = pgTable('reactions', {
  id: serial('id').primaryKey(),
  emoji: text('emoji').notNull(),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  commentId: integer('commentId')
    .notNull()
    .references(() => pgComments.id, { onDelete: 'cascade' }),
})

export const pgAttachments = pgTable('attachments', {
  id: serial('id').primaryKey(),
  filename: text('filename').notNull(),
  url: text('url').notNull(),
  mimeType: text('mimeType').notNull(),
  size: integer('size').notNull(),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  taskId: integer('taskId')
    .notNull()
    .references(() => pgTasks.id, { onDelete: 'cascade' }),
})

export const pgActivities = pgTable('activities', {
  id: serial('id').primaryKey(),
  action: text('action').notNull(),
  details: json('details'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  taskId: integer('taskId').references(() => pgTasks.id, {
    onDelete: 'cascade',
  }),
  userId: integer('userId')
    .notNull()
    .references(() => pgUsers.id, { onDelete: 'cascade' }),
})

export const pgNotifications = pgTable('notifications', {
  id: serial('id').primaryKey(),
  type: text('type').notNull(),
  title: text('title').notNull(),
  message: text('message'),
  isRead: boolean('isRead').notNull().default(false),
  data: json('data'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  userId: integer('userId')
    .notNull()
    .references(() => pgUsers.id, { onDelete: 'cascade' }),
})

// SQLite Schema
export const sqliteOrganizations = sqliteTable('organizations', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  name: sqliteText('name').notNull(),
  slug: sqliteText('slug').notNull().unique(),
  plan: sqliteText('plan').notNull().default('FREE'),
  settings: sqliteText('settings'),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
})

export const sqliteUsers = sqliteTable('users', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  email: sqliteText('email').notNull().unique(),
  name: sqliteText('name'),
  avatarUrl: sqliteText('avatarUrl'),
  role: sqliteText('role').notNull().default('USER'),
  status: sqliteText('status').notNull().default('ACTIVE'),
  metadata: sqliteText('metadata'),
  tags: sqliteText('tags').notNull().default('[]'),
  lastLoginAt: sqliteInteger('lastLoginAt', { mode: 'timestamp' }),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
})

export const sqliteProjects = sqliteTable('projects', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  name: sqliteText('name').notNull(),
  description: sqliteText('description'),
  color: sqliteText('color'),
  status: sqliteText('status').notNull().default('ACTIVE'),
  isPublic: sqliteInteger('isPublic', { mode: 'boolean' })
    .notNull()
    .default(false),
  budget: sqliteReal('budget'),
  startDate: sqliteInteger('startDate', { mode: 'timestamp' }),
  endDate: sqliteInteger('endDate', { mode: 'timestamp' }),
  metadata: sqliteText('metadata'),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' }).notNull(),
  updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' }).notNull(),
  organizationId: sqliteInteger('organizationId').notNull(),
})

export const sqliteTasks: SQLiteTableWithColumns<any> = sqliteTable('tasks', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  title: sqliteText('title').notNull(),
  description: sqliteText('description'),
  status: sqliteText('status').notNull().default('TODO'),
  priority: sqliteText('priority').notNull().default('MEDIUM'),
  position: sqliteInteger('position').notNull().default(0),
  dueDate: sqliteInteger('dueDate', { mode: 'timestamp' }),
  completedAt: sqliteInteger('completedAt', { mode: 'timestamp' }),
  estimatedHours: sqliteReal('estimatedHours'),
  metadata: sqliteText('metadata'),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' }).notNull(),
  updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' }).notNull(),
  projectId: sqliteInteger('projectId').notNull(),
  assigneeId: sqliteInteger('assigneeId'),
  creatorId: sqliteInteger('creatorId').notNull(),
  milestoneId: sqliteInteger('milestoneId'),
  parentId: sqliteInteger('parentId'),
})

export const sqliteComments: SQLiteTableWithColumns<any> = sqliteTable(
  'comments',
  {
    id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
    content: sqliteText('content').notNull(),
    isEdited: sqliteInteger('isEdited', { mode: 'boolean' })
      .notNull()
      .default(false),
    createdAt: sqliteInteger('createdAt', { mode: 'timestamp' }).notNull(),
    updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' }).notNull(),
    taskId: sqliteInteger('taskId').notNull(),
    authorId: sqliteInteger('authorId').notNull(),
    parentId: sqliteInteger('parentId'),
  },
)
