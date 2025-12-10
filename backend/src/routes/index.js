import { Router } from "express";
import healtcheckRoutes from './health.js'
import admin from './admin.js'
import superAdmin from './superAdmin.js'
import scanner from './scanner.js'

const router = Router()

router.use("/healtcheck", healtcheckRoutes)
router.use("/scanner", scanner)

export default router