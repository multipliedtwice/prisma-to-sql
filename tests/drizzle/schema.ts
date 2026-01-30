import {
  pgTable,
  serial,
  varchar,
  timestamp,
  boolean,
  integer,
  text,
  jsonb,
  decimal,
  primaryKey,
} from 'drizzle-orm/pg-core'
import {
  sqliteTable,
  integer as sqliteInteger,
  text as sqliteText,
  real as sqliteReal,
} from 'drizzle-orm/sqlite-core'
import { relations, sql } from 'drizzle-orm'

// ============================================================================
// PostgreSQL Tables
// ============================================================================

export const pgUsers = pgTable('users', {
  id: serial('id').primaryKey(),
  email: varchar('email', { length: 255 }).notNull().unique(),
  name: varchar('name', { length: 255 }),
  avatarUrl: varchar('avatar_url', { length: 255 }),
  role: varchar('role', { length: 50 }).notNull().default('USER'),
  status: varchar('status', { length: 50 }).notNull().default('ACTIVE'),
  isDeleted: boolean('is_deleted').notNull().default(false),
  metadata: text('metadata'),
  tags: text('tags').notNull().default('[]'),
  lastLoginAt: timestamp('lastLoginAt'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
})

export const pgOrganizations = pgTable('organizations', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 255 }).notNull(),
  slug: varchar('slug', { length: 255 }).notNull().unique(),
  plan: varchar('plan', { length: 50 }).notNull().default('FREE'),
  settings: jsonb('settings'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
})

export const pgMembers = pgTable('members', {
  id: serial('id').primaryKey(),
  role: varchar('role', { length: 50 }).notNull().default('MEMBER'),
  joinedAt: timestamp('joinedAt').notNull().defaultNow(),
  organizationId: integer('organizationId')
    .notNull()
    .references(() => pgOrganizations.id),
  userId: integer('userId')
    .notNull()
    .references(() => pgUsers.id),
})

export const pgProjects = pgTable('projects', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 255 }).notNull(),
  description: text('description'),
  color: varchar('color', { length: 50 }),
  status: varchar('status', { length: 50 }).notNull().default('ACTIVE'),
  isPublic: boolean('isPublic').notNull().default(false),
  budget: decimal('budget'),
  startDate: timestamp('startDate'),
  endDate: timestamp('endDate'),
  metadata: jsonb('metadata'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
  organizationId: integer('organizationId')
    .notNull()
    .references(() => pgOrganizations.id),
})

export const pgMilestones = pgTable('milestones', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 255 }).notNull(),
  description: text('description'),
  dueDate: timestamp('dueDate'),
  completedAt: timestamp('completedAt'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  projectId: integer('projectId')
    .notNull()
    .references(() => pgProjects.id),
})

export const pgLabels = pgTable('labels', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 255 }).notNull(),
  color: varchar('color', { length: 50 }).notNull(),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  projectId: integer('projectId')
    .notNull()
    .references(() => pgProjects.id),
})

export const pgTasks = pgTable('tasks', {
  id: serial('id').primaryKey(),
  title: varchar('title', { length: 255 }).notNull(),
  description: text('description'),
  status: varchar('status', { length: 50 }).notNull().default('TODO'),
  priority: varchar('priority', { length: 50 }).notNull().default('MEDIUM'),
  position: integer('position').notNull().default(0),
  dueDate: timestamp('dueDate'),
  completedAt: timestamp('completedAt'),
  estimatedHours: decimal('estimatedHours'),
  metadata: jsonb('metadata'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
  projectId: integer('projectId')
    .notNull()
    .references(() => pgProjects.id),
  assigneeId: integer('assigneeId').references(() => pgUsers.id),
  creatorId: integer('creatorId')
    .notNull()
    .references(() => pgUsers.id),
  milestoneId: integer('milestoneId').references(() => pgMilestones.id),
  parentId: integer('parentId'),
})

export const pgTaskLabels = pgTable(
  'task_labels',
  {
    taskId: integer('taskId')
      .notNull()
      .references(() => pgTasks.id),
    labelId: integer('labelId')
      .notNull()
      .references(() => pgLabels.id),
    assignedAt: timestamp('assignedAt').notNull().defaultNow(),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.taskId, table.labelId] }),
  }),
)

export const pgComments = pgTable('comments', {
  id: serial('id').primaryKey(),
  content: text('content').notNull(),
  isEdited: boolean('isEdited').notNull().default(false),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  updatedAt: timestamp('updatedAt').notNull().defaultNow(),
  taskId: integer('taskId')
    .notNull()
    .references(() => pgTasks.id),
  authorId: integer('authorId')
    .notNull()
    .references(() => pgUsers.id),
  parentId: integer('parentId'),
})

export const pgReactions = pgTable('reactions', {
  id: serial('id').primaryKey(),
  emoji: varchar('emoji', { length: 10 }).notNull(),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  commentId: integer('commentId')
    .notNull()
    .references(() => pgComments.id),
})

export const pgAttachments = pgTable('attachments', {
  id: serial('id').primaryKey(),
  filename: varchar('filename', { length: 255 }).notNull(),
  url: varchar('url', { length: 500 }).notNull(),
  mimeType: varchar('mimeType', { length: 100 }).notNull(),
  size: integer('size').notNull(),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  taskId: integer('taskId')
    .notNull()
    .references(() => pgTasks.id),
})

export const pgActivities = pgTable('activities', {
  id: serial('id').primaryKey(),
  action: varchar('action', { length: 100 }).notNull(),
  details: jsonb('details'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  taskId: integer('taskId').references(() => pgTasks.id),
  userId: integer('userId')
    .notNull()
    .references(() => pgUsers.id),
})

export const pgNotifications = pgTable('notifications', {
  id: serial('id').primaryKey(),
  type: varchar('type', { length: 100 }).notNull(),
  title: varchar('title', { length: 255 }).notNull(),
  message: text('message'),
  isRead: boolean('isRead').notNull().default(false),
  data: jsonb('data'),
  createdAt: timestamp('createdAt').notNull().defaultNow(),
  userId: integer('userId')
    .notNull()
    .references(() => pgUsers.id),
})

// ============================================================================
// PostgreSQL Relations
// ============================================================================

export const pgUsersRelations = relations(pgUsers, ({ many }) => ({
  assignedTasks: many(pgTasks, { relationName: 'assignee' }),
  createdTasks: many(pgTasks, { relationName: 'creator' }),
  comments: many(pgComments),
  activities: many(pgActivities),
  notifications: many(pgNotifications),
  memberships: many(pgMembers),
}))

export const pgOrganizationsRelations = relations(
  pgOrganizations,
  ({ many }) => ({
    members: many(pgMembers),
    projects: many(pgProjects),
  }),
)

export const pgMembersRelations = relations(pgMembers, ({ one }) => ({
  organization: one(pgOrganizations, {
    fields: [pgMembers.organizationId],
    references: [pgOrganizations.id],
  }),
  user: one(pgUsers, {
    fields: [pgMembers.userId],
    references: [pgUsers.id],
  }),
}))

export const pgProjectsRelations = relations(pgProjects, ({ one, many }) => ({
  organization: one(pgOrganizations, {
    fields: [pgProjects.organizationId],
    references: [pgOrganizations.id],
  }),
  tasks: many(pgTasks),
  labels: many(pgLabels),
  milestones: many(pgMilestones),
}))

export const pgMilestonesRelations = relations(
  pgMilestones,
  ({ one, many }) => ({
    project: one(pgProjects, {
      fields: [pgMilestones.projectId],
      references: [pgProjects.id],
    }),
    tasks: many(pgTasks),
  }),
)

export const pgLabelsRelations = relations(pgLabels, ({ one, many }) => ({
  project: one(pgProjects, {
    fields: [pgLabels.projectId],
    references: [pgProjects.id],
  }),
  taskLabels: many(pgTaskLabels),
}))

export const pgTasksRelations = relations(pgTasks, ({ one, many }) => ({
  project: one(pgProjects, {
    fields: [pgTasks.projectId],
    references: [pgProjects.id],
  }),
  assignee: one(pgUsers, {
    fields: [pgTasks.assigneeId],
    references: [pgUsers.id],
    relationName: 'assignee',
  }),
  creator: one(pgUsers, {
    fields: [pgTasks.creatorId],
    references: [pgUsers.id],
    relationName: 'creator',
  }),
  milestone: one(pgMilestones, {
    fields: [pgTasks.milestoneId],
    references: [pgMilestones.id],
  }),
  comments: many(pgComments),
  activities: many(pgActivities),
  attachments: many(pgAttachments),
  labels: many(pgTaskLabels),
}))

export const pgTaskLabelsRelations = relations(pgTaskLabels, ({ one }) => ({
  task: one(pgTasks, {
    fields: [pgTaskLabels.taskId],
    references: [pgTasks.id],
  }),
  label: one(pgLabels, {
    fields: [pgTaskLabels.labelId],
    references: [pgLabels.id],
  }),
}))

export const pgCommentsRelations = relations(pgComments, ({ one, many }) => ({
  task: one(pgTasks, {
    fields: [pgComments.taskId],
    references: [pgTasks.id],
  }),
  author: one(pgUsers, {
    fields: [pgComments.authorId],
    references: [pgUsers.id],
  }),
  reactions: many(pgReactions),
}))

export const pgReactionsRelations = relations(pgReactions, ({ one }) => ({
  comment: one(pgComments, {
    fields: [pgReactions.commentId],
    references: [pgComments.id],
  }),
}))

export const pgAttachmentsRelations = relations(pgAttachments, ({ one }) => ({
  task: one(pgTasks, {
    fields: [pgAttachments.taskId],
    references: [pgTasks.id],
  }),
}))

export const pgActivitiesRelations = relations(pgActivities, ({ one }) => ({
  task: one(pgTasks, {
    fields: [pgActivities.taskId],
    references: [pgTasks.id],
  }),
  user: one(pgUsers, {
    fields: [pgActivities.userId],
    references: [pgUsers.id],
  }),
}))

export const pgNotificationsRelations = relations(
  pgNotifications,
  ({ one }) => ({
    user: one(pgUsers, {
      fields: [pgNotifications.userId],
      references: [pgUsers.id],
    }),
  }),
)

// ============================================================================
// SQLite Tables
// ============================================================================

export const sqliteUsers = sqliteTable('users', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  email: sqliteText('email').notNull().unique(),
  name: sqliteText('name'),
  avatarUrl: sqliteText('avatar_url'),
  role: sqliteText('role').notNull().default('USER'),
  status: sqliteText('status').notNull().default('ACTIVE'),
  isDeleted: sqliteInteger('is_deleted', { mode: 'boolean' })
    .notNull()
    .default(false),
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

export const sqliteMembers = sqliteTable('members', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  role: sqliteText('role').notNull().default('MEMBER'),
  joinedAt: sqliteInteger('joinedAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  organizationId: sqliteInteger('organizationId').notNull(),
  userId: sqliteInteger('userId').notNull(),
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
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  organizationId: sqliteInteger('organizationId').notNull(),
})

export const sqliteMilestones = sqliteTable('milestones', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  name: sqliteText('name').notNull(),
  description: sqliteText('description'),
  dueDate: sqliteInteger('dueDate', { mode: 'timestamp' }),
  completedAt: sqliteInteger('completedAt', { mode: 'timestamp' }),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  projectId: sqliteInteger('projectId').notNull(),
})

export const sqliteLabels = sqliteTable('labels', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  name: sqliteText('name').notNull(),
  color: sqliteText('color').notNull(),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  projectId: sqliteInteger('projectId').notNull(),
})

export const sqliteTasks = sqliteTable('tasks', {
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
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  projectId: sqliteInteger('projectId').notNull(),
  assigneeId: sqliteInteger('assigneeId'),
  creatorId: sqliteInteger('creatorId').notNull(),
  milestoneId: sqliteInteger('milestoneId'),
  parentId: sqliteInteger('parentId'),
})

export const sqliteTaskLabels = sqliteTable('task_labels', {
  taskId: sqliteInteger('taskId').notNull(),
  labelId: sqliteInteger('labelId').notNull(),
  assignedAt: sqliteInteger('assignedAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
})

export const sqliteComments = sqliteTable('comments', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  content: sqliteText('content').notNull(),
  isEdited: sqliteInteger('isEdited', { mode: 'boolean' })
    .notNull()
    .default(false),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  updatedAt: sqliteInteger('updatedAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  taskId: sqliteInteger('taskId').notNull(),
  authorId: sqliteInteger('authorId').notNull(),
  parentId: sqliteInteger('parentId'),
})

export const sqliteReactions = sqliteTable('reactions', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  emoji: sqliteText('emoji').notNull(),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  commentId: sqliteInteger('commentId').notNull(),
})

export const sqliteAttachments = sqliteTable('attachments', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  filename: sqliteText('filename').notNull(),
  url: sqliteText('url').notNull(),
  mimeType: sqliteText('mimeType').notNull(),
  size: sqliteInteger('size').notNull(),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  taskId: sqliteInteger('taskId').notNull(),
})

export const sqliteActivities = sqliteTable('activities', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  action: sqliteText('action').notNull(),
  details: sqliteText('details'),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  taskId: sqliteInteger('taskId'),
  userId: sqliteInteger('userId').notNull(),
})

export const sqliteNotifications = sqliteTable('notifications', {
  id: sqliteInteger('id').primaryKey({ autoIncrement: true }),
  type: sqliteText('type').notNull(),
  title: sqliteText('title').notNull(),
  message: sqliteText('message'),
  isRead: sqliteInteger('isRead', { mode: 'boolean' }).notNull().default(false),
  data: sqliteText('data'),
  createdAt: sqliteInteger('createdAt', { mode: 'timestamp' })
    .notNull()
    .default(sql`(unixepoch())`),
  userId: sqliteInteger('userId').notNull(),
})

// ============================================================================
// SQLite Relations
// ============================================================================

export const sqliteUsersRelations = relations(sqliteUsers, ({ many }) => ({
  assignedTasks: many(sqliteTasks, { relationName: 'assignee' }),
  createdTasks: many(sqliteTasks, { relationName: 'creator' }),
  comments: many(sqliteComments),
  activities: many(sqliteActivities),
  notifications: many(sqliteNotifications),
  memberships: many(sqliteMembers),
}))

export const sqliteOrganizationsRelations = relations(
  sqliteOrganizations,
  ({ many }) => ({
    members: many(sqliteMembers),
    projects: many(sqliteProjects),
  }),
)

export const sqliteMembersRelations = relations(sqliteMembers, ({ one }) => ({
  organization: one(sqliteOrganizations, {
    fields: [sqliteMembers.organizationId],
    references: [sqliteOrganizations.id],
  }),
  user: one(sqliteUsers, {
    fields: [sqliteMembers.userId],
    references: [sqliteUsers.id],
  }),
}))

export const sqliteProjectsRelations = relations(
  sqliteProjects,
  ({ one, many }) => ({
    organization: one(sqliteOrganizations, {
      fields: [sqliteProjects.organizationId],
      references: [sqliteOrganizations.id],
    }),
    tasks: many(sqliteTasks),
    labels: many(sqliteLabels),
    milestones: many(sqliteMilestones),
  }),
)

export const sqliteMilestonesRelations = relations(
  sqliteMilestones,
  ({ one, many }) => ({
    project: one(sqliteProjects, {
      fields: [sqliteMilestones.projectId],
      references: [sqliteProjects.id],
    }),
    tasks: many(sqliteTasks),
  }),
)

export const sqliteLabelsRelations = relations(
  sqliteLabels,
  ({ one, many }) => ({
    project: one(sqliteProjects, {
      fields: [sqliteLabels.projectId],
      references: [sqliteProjects.id],
    }),
    taskLabels: many(sqliteTaskLabels),
  }),
)

export const sqliteTasksRelations = relations(sqliteTasks, ({ one, many }) => ({
  project: one(sqliteProjects, {
    fields: [sqliteTasks.projectId],
    references: [sqliteProjects.id],
  }),
  assignee: one(sqliteUsers, {
    fields: [sqliteTasks.assigneeId],
    references: [sqliteUsers.id],
    relationName: 'assignee',
  }),
  creator: one(sqliteUsers, {
    fields: [sqliteTasks.creatorId],
    references: [sqliteUsers.id],
    relationName: 'creator',
  }),
  milestone: one(sqliteMilestones, {
    fields: [sqliteTasks.milestoneId],
    references: [sqliteMilestones.id],
  }),
  comments: many(sqliteComments),
  activities: many(sqliteActivities),
  attachments: many(sqliteAttachments),
  labels: many(sqliteTaskLabels),
}))

export const sqliteTaskLabelsRelations = relations(
  sqliteTaskLabels,
  ({ one }) => ({
    task: one(sqliteTasks, {
      fields: [sqliteTaskLabels.taskId],
      references: [sqliteTasks.id],
    }),
    label: one(sqliteLabels, {
      fields: [sqliteTaskLabels.labelId],
      references: [sqliteLabels.id],
    }),
  }),
)

export const sqliteCommentsRelations = relations(
  sqliteComments,
  ({ one, many }) => ({
    task: one(sqliteTasks, {
      fields: [sqliteComments.taskId],
      references: [sqliteTasks.id],
    }),
    author: one(sqliteUsers, {
      fields: [sqliteComments.authorId],
      references: [sqliteUsers.id],
    }),
    reactions: many(sqliteReactions),
  }),
)

export const sqliteReactionsRelations = relations(
  sqliteReactions,
  ({ one }) => ({
    comment: one(sqliteComments, {
      fields: [sqliteReactions.commentId],
      references: [sqliteComments.id],
    }),
  }),
)

export const sqliteAttachmentsRelations = relations(
  sqliteAttachments,
  ({ one }) => ({
    task: one(sqliteTasks, {
      fields: [sqliteAttachments.taskId],
      references: [sqliteTasks.id],
    }),
  }),
)

export const sqliteActivitiesRelations = relations(
  sqliteActivities,
  ({ one }) => ({
    task: one(sqliteTasks, {
      fields: [sqliteActivities.taskId],
      references: [sqliteTasks.id],
    }),
    user: one(sqliteUsers, {
      fields: [sqliteActivities.userId],
      references: [sqliteUsers.id],
    }),
  }),
)

export const sqliteNotificationsRelations = relations(
  sqliteNotifications,
  ({ one }) => ({
    user: one(sqliteUsers, {
      fields: [sqliteNotifications.userId],
      references: [sqliteUsers.id],
    }),
  }),
)
